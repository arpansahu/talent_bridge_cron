import logging
from datetime import datetime
from unidecode import unidecode
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import Q
import boto3
from companies.models import Company
from locations.models import Locations
from jobs.models import Jobs
from scrapy.utils.project import get_project_settings  # Import Scrapy settings


class JobsPipeline:

    def __init__(self):
        self.logger = None
        self.log_file_name = None
        settings = get_project_settings()  # Get the Scrapy settings

        # Setup S3 client using settings from Scrapy
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.get('AWS_SECRET_ACCESS_KEY')
        )

        # Store the bucket name and other settings for reuse
        self.bucket_name = settings.get('AWS_STORAGE_BUCKET_NAME')
        self.profile_name = "profile"  # Replace with the actual profile name
        self.project_name = settings.get('PROJECT_NAME')  # Replace with the actual project name

    def open_spider(self, spider):
        # Initialize logger with spider's name and date
        current_date = datetime.now().strftime('%Y%m%d')
        self.log_file_name = f"{spider.name}_{current_date}.log"
        logging.basicConfig(filename=self.log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(spider.name)
        self.logger.info("Spider started: %s", spider.name)

    async def process_item(self, item, spider):
        try:
            await write_item_to_db(item, self.logger)
        except Exception as e:
            self.logger.error("Failed to process item: %s", e)
        return item

    def close_spider(self, spider):
        # Final log entry
        self.logger.info("Spider finished: %s", spider.name)
        # Upload log file to S3 bucket
        self.upload_log_to_s3(spider)

    def upload_log_to_s3(self, spider):
        # Construct the S3 object path
        current_date = datetime.now().strftime('%Y%m%d')
        object_name = f"{self.profile_name}/{self.project_name}/logs/{spider.name}/{spider.name}_{current_date}.log"

        try:
            self.s3_client.upload_file(self.log_file_name, self.bucket_name, object_name)
            self.logger.info("Successfully uploaded log file to S3: %s", object_name)
        except Exception as e:
            self.logger.error("Failed to upload log file to S3: %s", e)

@sync_to_async
@transaction.atomic
def write_item_to_db(item, logger):
    logger.info("Processing item with job id %s", item['job_id'])

    # Check if the job already exists
    if not Jobs.objects.filter(company__name=item['company'], job_id=item['job_id']).exists():
        logger.info(f"JOB url: {item['job_url']}")
        logger.debug(f"Item details: {item}")

        locations_objects_array, locations_str = process_locations(item['locations'], logger)

        try:
            # Retrieve the company object
            company = Company.objects.get(name=item['company'])
            logger.info(f"Company {item['company']} found")
        except Company.DoesNotExist:
            logger.error("Company %s does not exist, cannot process job %s", item['company'], item['job_id'])
            return

        # Ensure category and sub_category have values
        category = item.get('category', '') or ''
        sub_category = item.get('sub_category', '') or ''

        # Convert the item dictionary to a Jobs instance using the correct field names
        job_instance = Jobs(
            company=company,
            job_id=item['job_id'],
            title=item['title'],
            category=category,
            sub_category=sub_category,
            job_url=item['job_url'],
            post=item['post'],
            location_str=locations_str
        )

        # Save the job instance to the database
        job_instance.save()
        logger.info("Added Job with job id %s", job_instance.job_id)

        # Assign the locations to the job item
        job_instance.location.add(*locations_objects_array)
        logger.info("Updated Locations for job id %s", job_instance.job_id)
    else:
        logger.info("Job with job id %s already exists", item['job_id'])


def process_locations(locations, logger):
    logger.info("Processing locations")
    locations_objects_array = []
    locations_str = None

    for loc in locations:
        try:
            parts = [unidecode(part) for part in loc.split(', ')]
            logger.debug(f"Processing parts: {parts}")

            city = None
            state = None
            country = None

            for part in parts:
                logger.debug(f"Processing part: {part}")
                # Check if this part matches a city
                if not city and Locations.objects.filter(city=part).exists():
                    city = part
                    logger.info(f"Matched as city: {city}")
                # Check if this part matches a state
                elif not state and Locations.objects.filter(state=part).exists():
                    state = part
                    logger.info(f"Matched as state: {state}")
                # Check if this part matches a country
                elif not country and Locations.objects.filter(country=part).exists():
                    country = part
                    logger.info(f"Matched as country: {country}")

                # Check if this part matches an ISO code (iso2 or iso3)
                elif not country and Locations.objects.filter(country_code_iso2=part).exists():
                    country = Locations.objects.filter(country_code_iso2=part).first().country
                    logger.info(f"Matched as ISO2 country: {country}")
                elif not country and Locations.objects.filter(country_code_iso3=part).exists():
                    country = Locations.objects.filter(country_code_iso3=part).first().country
                    logger.info(f"Matched as ISO3 country: {country}")

            logger.info(f"Final location: city={city}, state={state}, country={country}")
            # Construct a query to find the location object
            location_object = find_location(city, state, country)

            if location_object:
                logger.info(f"Location found and appended: {location_object}")
                locations_objects_array.append(location_object)
            else:
                logger.warning(f"Not a valid location: city={city}, state={state}, country={country} for location string: {loc}")
                locations_str = loc

        except ValueError as e:
            logger.error(f"Failed to process location {loc}: {str(e)}")
            locations_str = loc
            save_unknown_location(loc, logger)

    return locations_objects_array, locations_str


def find_location(city=None, state=None, country=None):
    filters = Q()
    if city:
        filters &= Q(city=city)
    if state:
        filters &= Q(state=state)
    if country:
        filters &= Q(country=country)

    return Locations.objects.filter(filters).first()


def save_unknown_location(city, country_or_state, logger):
    with open("unknown_locations.txt", "a") as f:
        f.write(f"Unable to add new location: {city}, {country_or_state}\n")
    logger.warning(f"Saved unknown location: {city}, {country_or_state}")
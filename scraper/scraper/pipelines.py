import logging
from unidecode import unidecode
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import Q
from companies.models import Company
from locations.models import Locations
from jobs.models import Jobs, JobLocation
from scrapy.exceptions import IgnoreRequest
import asyncio 

class JobsPipeline:

    def open_spider(self, spider):
        self.logger = logging.getLogger(spider.name)
        self.logger.info("Pipeline initialized for spider: %s", spider.name)

    def process_item(self, item, spider):
        try:
            # Schedule the coroutine to be run by the event loop
            asyncio.create_task(self.write_item(item, spider))
        except Exception as e:
            spider.progress_bar.update(1)
            self.logger.error("Failed to process item: %s", e)
        return item

    async def write_item(self, item, spider):
        try:
            await write_item_to_db(item, self.logger)
        finally:
            spider.progress_bar.update(1)  # Ensure progress bar updates regardless of success or failure
            self.log_progress(spider)
            
    def log_progress(self, spider):
        # Log the progress bar status
        self.logger.info(f"Progress: {spider.progress_bar.n}/{spider.progress_bar.total} items processed.")

    def close_spider(self, spider):
        self.logger.info("Pipeline closing for spider: %s", spider.name)

@sync_to_async
@transaction.atomic
def write_item_to_db(item, logger):
    logger.info("Processing item with job id %s", item['locations'])

    # Check if the job already exists
    if not Jobs.objects.filter(company__name=item['company'], job_id=item['job_id']).exists():
        logger.info(f"JOB url: {item['job_url']}")
        logger.info(f"JOB locations: {item['locations']}")

        locations_objects_array = process_locations(item['locations'], logger,item['job_url'])
        
        logger.info(f"==============Locations are processed successfully {locations_objects_array}=============")

        any_remote = any(obj['remote'] for obj in locations_objects_array)
        any_non_remote = any(not obj['remote'] for obj in locations_objects_array)

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
            remote=any_remote,
            in_office=any_non_remote
        )

        # Save the job instance to the database
        job_instance.save()
        logger.info("Added Job with job id %s", job_instance.job_id)

        # Assign the locations to the job item
        for location_data in locations_objects_array:
            JobLocation.objects.create(
                job=job_instance,
                location=location_data['location_object'],
                remote=location_data['remote']
            )
            logger.info(
                "Added location %s (remote: %s) to job id %s", 
                location_data['location_object'], 
                location_data['remote'], 
                job_instance.job_id,
            )

        logger.info("Updated Locations for job id %s", job_instance.job_id)
    else:
        logger.info("Job with job id %s already exists", item['job_id'])

def process_locations(locations, logger, job_url):
    logger.info(f"Processing locations {locations}")
    locations_objects_array = []

    logger.info("Calling For loop")
    for loc in locations:
        try:
            logger.info("checkpoint 1")
            loc_element = loc['location']
            loc_remote = loc['remote']    
            logger.info("checkpoint 2")
            loc_dict = {
                'location_object': None,
                'remote': loc_remote,
            }
            logger.info("checkpoint 3")
            parts = [unidecode(part) for part in loc_element.split(', ')]
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
                loc_dict['location_object'] = location_object
            else:
                logger.warning(f"Location Object Not Found for {loc_element} Try to Create a New location")
                
                # Case When Remote is Present with Country
                if 'Remote' in loc_element:
                    location_object_created = create_remote_location(loc_element)

                    if location_object_created:
                        loc_dict['location_object'] = location_object_created

                else:    
                    logger.warning(f"Skipping item due to location: {loc_element}")
                    raise IgnoreRequest(f"Skipping item due to location: {loc_element} {job_url}")

            locations_objects_array.append(loc_dict)

        except ValueError as e:
            logger.error(f"Failed to process location {loc}: {str(e)}")
            locations_str = loc
            save_unknown_location(loc, logger)

    return locations_objects_array

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
    logger.warning(f"Unknown location: city={city}, state/country={country_or_state}")

def create_remote_location(loc_element):
    if 'Remote' in loc_element and loc_element.count(',')==2:
        city, state, country = loc_element.split(',')
        country_iso2 = Locations.objects.get(country=country).first().country_code_iso2
        country_iso3 = Locations.objects.get(country=country).first().country_code_iso3
        
        location_instance = Locations(
            city=city,
            country=country,
            country_code_iso2=country_iso2,
            country_code_iso3=country_iso3,
            state = state,
        )

        logger.info(f"New Remote Location Created for : {loc_element}")
        location_instance.save()
        logger.info("Added Location with location_instance id %s", location_instance.id)

        return location_instance


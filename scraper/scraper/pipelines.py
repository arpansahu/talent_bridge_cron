import coloredlogs
import logging
from unidecode import unidecode
from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import Q
from companies.models import Company
from locations.models import Locations
from jobs.models import Jobs

# Setting up the logger
logger = logging.getLogger(__name__)
coloredlogs.install(level="WARN", logger=logger)

@sync_to_async
@transaction.atomic
def write_item_to_db(item):
    logger.info("Processing item with job id %s", item['job_id'])

    # Check if the job already exists
    if not Jobs.objects.filter(company__name=item['company'], job_id=item['job_id']).exists():
        print(f"===========================JOB url: {item['job_url']}=====================================")
        print(item)
        locations_objects_array, locations_str = process_locations(item['locations'])

        try:
            # Retrieve the company object
            company = Company.objects.get(name=item['company'])
        except Company.DoesNotExist:
            logger.error("Company %s does not exist, cannot process job %s", item['company'], item['job_id'])
            return

        # Ensure category and sub_category have values
        category = item.get('category', '') or ''
        sub_category = item.get('sub_category', '') or ''  # Correct key 'sub_category'

        # Convert the item dictionary to a Jobs instance using the correct field names
        job_instance = Jobs(
            company=company,
            job_id=item['job_id'],
            title=item['title'],
            category=category,
            sub_category=sub_category,  # Correct field name used here
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


def process_locations(locations):
    locations_objects_array = []
    locations_str = None

    for loc in locations:
        try:
            parts = [unidecode(part) for part in loc.split(', ')]
            print(f"===========================parts: {parts}=====================================")
    
            city = None
            state = None
            country = None

            count=1
            for part in parts:
                print(f"===========================part: {part}=====================================")
                # Check if this part matches a city
                if not city and Locations.objects.filter(city=part).exists():
                    city = part
                    print(f"===========================it is a city: {city}=====================================")
                # Check if this part matches a state
                elif not state and Locations.objects.filter(state=part).exists():
                    state = part
                    print(f"===========================it is a state: {state}=====================================")
                # Check if this part matches a country
                elif not country and Locations.objects.filter(country=part).exists():
                    country = part
                    print(f"===========================it is a country: {country}=====================================")

                # Check if this part matches an ISO code (iso2 or iso3)
                elif not country and Locations.objects.filter(country_code_iso2=part).exists():
                    country = Locations.objects.filter(country_code_iso2=part).first().country
                    print(f"===========country_code_iso2================it is a country: {country}=====================================")
                elif not country and Locations.objects.filter(country_code_iso3=part).exists():
                    country = Locations.objects.filter(country_code_iso3=part).first().country
                    print(f"===========country_code_iso3================it is a country: {country}=====================================")
                
                count += 1

            print(f"==================city : {city}=====state : {state}=================country : {country}========================")
            # Construct a query to find the location object
            location_object = Locations.objects.filter(
                Q(city=city) |
                Q(state=state) |
                Q(country=country)
            ).first()

            if location_object:
                print("==========================Appended the Location======================================")
                locations_objects_array.append(location_object)
            else:
                print("==========================Not a Valid Location======================================")
                logger.warning(f"Not a valid location: city={city}, state={state}, country={country} for location string: {loc}")
                locations_str = loc
                # Handle new location creation if not found
                # locations_str = handle_new_location(city, state or country)

        except ValueError:
            logger.error("Failed to unpack location %s", loc)
            locations_str = loc
            save_unknown_location(loc)
    
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

def handle_new_location(city, country_or_state):
    # Check if the city is 'Remote'
    if city == 'Remote':
        return create_remote_location(city, country_or_state)
    
    # Try to find an existing location before creating a new one
    existing_location = Locations.objects.filter(
        city=city, 
        country=country_or_state,
        state=country_or_state
    ).first()

    if existing_location:
        logger.info("Location already exists: %s, %s, %s", city, country_or_state, country_or_state)
        return existing_location

    logger.info("Adding new location for %s, %s", city, country_or_state)
    location_details = Locations.objects.filter(
        Q(country=country_or_state) |
        Q(country_code_iso2=country_or_state) |
        Q(country_code_iso3=country_or_state) |
        Q(state=country_or_state) |
        Q(state_code=country_or_state)
    ).first()

    if location_details:
        location = Locations.objects.create(
            city=city,
            country=location_details.country,
            country_code_iso3=location_details.country_code_iso3,
            country_code_iso2=location_details.country_code_iso2,
            state=location_details.state,
            state_code=location_details.state_code,
        )
        log_new_location(city, country_or_state)
        return location
    else:
        save_unknown_location(city, country_or_state)
        return None

def create_remote_location(city, country_or_state):
    logger.info("Creating new Remote location for %s, %s", city, country_or_state)
    country_details = Locations.objects.filter(country=country_or_state).first()
    if country_details:
        return Locations.objects.create(
            city=city,
            country=country_or_state,
            country_code_iso3=country_details.country_code_iso3,
            country_code_iso2=country_details.country_code_iso2,
            state=city
        )
    return None

def add_new_location(city, country_or_state):
    logger.info("Adding new location for %s, %s", city, country_or_state)
    location_details = Locations.objects.filter(
        Q(country=country_or_state) |
        Q(country_code_iso2=country_or_state) |
        Q(country_code_iso3=country_or_state) |
        Q(state=country_or_state) |
        Q(state_code=country_or_state)
    ).first()

    if location_details:
        location = Locations.objects.create(
            city=city,
            country=location_details.country,
            country_code_iso3=location_details.country_code_iso3,
            country_code_iso2=location_details.country_code_iso2,
            state=location_details.state,
            state_code=location_details.state_code,
        )
        log_new_location(city, country_or_state)
        return location
    else:
        save_unknown_location(city, country_or_state)
        return None

def log_new_location(city, country_or_state):
    with open("new_locations.txt", "a") as f:
        f.write(f"Added new location: {city}, {country_or_state}\n")

def save_unknown_location(city, country_or_state):
    with open("unknown_locations.txt", "a") as f:
        f.write(f"Unable to add new location: {city}, {country_or_state}\n")

class JobsPipeline:

    async def process_item(self, item, spider):
        try:
            await write_item_to_db(item)
        except Exception as e:
            logger.error("Failed to process item: %s", e)
        return item
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import coloredlogs
import logging
from unidecode import unidecode
logger = logging.getLogger(__name__)
coloredlogs.install(level="WARN", logger=logger)

from asgiref.sync import sync_to_async
from companies.models import Company
from locations.models import Locations
from jobs.models import Jobs


@sync_to_async
def write_item_to_db(item):
    print("sync_func() was CALLED (good!)")

    if not Jobs.objects.filter(company__name=item['company'], job_id=item['job_id']).first():
        locations = item['locations']
        del item['locations']

        locations_objects_array = []
        locations_str = None

        for loc in locations:
            try:
                city, country_or_state = loc.split(', ')

                city = unidecode(city)
                country_or_state = unidecode(city)

                location_object = Locations.objects.filter(city=city, country=country_or_state).first()

                if location_object:
                    locations_objects_array.append(location_object)
                    break

                location_object = Locations.objects.filter(city=city, country_code_iso2=country_or_state).first()

                if location_object:
                    locations_objects_array.append(location_object)
                    break

                location_object = Locations.objects.filter(city=city, country_code_iso3=country_or_state).first()

                if location_object:
                    locations_objects_array.append(location_object)
                    break

                location_object = Locations.objects.filter(city=city, state=country_or_state).first()

                if location_object:
                    locations_objects_array.append(location_object)
                    break

                location_object = Locations.objects.filter(city=city, state_code=country_or_state).first()

                if location_object:
                    locations_objects_array.append(location_object)
                    break

                if city == 'Remote':
                    print("Locations Object not Found", city, country_or_state)
                    print("Remote city New Entry for Country", country_or_state)

                    location_object_details = Locations.objects.filter(country=country_or_state).first()

                    location_object = Locations.objects.create(
                        city=city,
                        country=country_or_state,
                        country_code_iso3=location_object_details.country_code_iso3,
                        country_code_iso2=location_object_details.country_code_iso2,
                        state=city
                    )

                    if location_object:
                        locations_objects_array.append(location_object)
                        break

                else:
                    print("Locations Object not Found", city, country_or_state)

                    location_object_details = Locations.objects.filter(country=country_or_state).first()

                    if not location_object_details:
                        location_object_details = Locations.objects.filter(country_code_iso2=country_or_state).first()

                    if not location_object_details:
                        location_object_details = Locations.objects.filter(country_code_iso3=country_or_state).first()

                    if not location_object_details:
                        location_object_details = Locations.objects.filter(state=country_or_state).first()

                    if not location_object_details:
                        location_object_details = Locations.objects.filter(state_code=country_or_state).first()

                    if location_object_details:
                        print("Adding New Location")

                        location_object = Locations.objects.create(
                            city=city,
                            country=location_object_details.country,
                            country_code_iso3=location_object_details.country_code_iso3,
                            country_code_iso2=location_object_details.country_code_iso2,
                            state=location_object_details.state,
                            state_code=location_object_details.state_code,
                        )

                        if location_object:
                            f = open("new_locations.txt", "a")
                            f.write(
                                f"Added new location based on following details {city} and {country_or_state}\n")
                            f.close()

                            locations_objects_array.append(location_object)
                            break
                    else:
                        print("Not able to add new location")

                        f = open("unknown_locations.txt", "a")
                        f.write(
                            f"Unable to add a new location based on following details {city} and {country_or_state}\n")
                        f.close()

            except Exception as e:
                print(e)
                f = open("city_only_locations.txt", "a")
                f.write(f"Unable to unpack loc with details {loc}\n")
                f.close()
                print("Only one location found in loc")

                locations_str = loc

        company = item['company']
        company_obj = Company.objects.get(name=company)

        item['company'] = company_obj
        item['location_str'] = locations_str
        item = item.save()

        print("Added Job with job id {}".format(item.job_id))

        print("Updating Locations")

        for loc in locations_objects_array:
            item.location.add(loc)

    else:
        print("Job with job id {} already exits".format(item['job_id']))


class JobsPipeline:

    async def process_item(self, item, spider):
        try:
            await write_item_to_db(item)
            # logger.warn("Added Job with id {}".format(item['job_id']))

        except Exception as e:
            print("\n")
            logger.error("\nFailed to load quote, Reason For Failure:{}".format(e))
        return item

from locations.models import Locations
from django.core.management.base import BaseCommand
import pandas as pd
import unicodedata

class Command(BaseCommand):
    def handle(self, *args, **options):
        print('----------------------------Started Location Creations--------------------------------')
        df = pd.read_csv('world_cities.csv')
        df = df.filter(items=['City', 'Country', 'ISO2', 'ISO3', 'State'])

        def normalize_text(text):
            # Normalize the text to remove any diacritical marks
            return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')

        for index, row in df.iterrows():
            try:
                print(f"--------Location no: {index}-----")
                print(row)

                # Normalize each field
                normalized_city = normalize_text(row['City'])
                normalized_country = normalize_text(row['Country'])
                normalized_iso2 = normalize_text(row['ISO2'])
                normalized_iso3 = normalize_text(row['ISO3'])
                normalized_state = normalize_text(row['State'])

                Locations.objects.create(
                    city=normalized_city,
                    country=normalized_country,
                    country_code_iso2=normalized_iso2,
                    country_code_iso3=normalized_iso3,
                    state=normalized_state
                )
                print("Created New Location Object")
            except Exception as e:
                print(f"Creation of New location Failed: Error is {e}")

        print('----------------------------Ended Location Creations--------------------------------')
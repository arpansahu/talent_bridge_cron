# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from scrapy_djangoitem import DjangoItem
from jobs.models import Jobs


class Job(DjangoItem):
    django_model = Jobs

    # Extra Fields
    locations = Field()

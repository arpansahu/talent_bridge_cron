from django.db import models

# Create your models here.
from jobportal_cron.models import AbstractBaseModel

class Locations(AbstractBaseModel):
    city = models.CharField(max_length=50)
    country = models.CharField(max_length=50)
    country_code_iso2 = models.CharField(max_length=2)  # Typically 2 characters
    country_code_iso3 = models.CharField(max_length=3)  # Typically 3 characters
    state = models.CharField(max_length=50)
    state_code = models.CharField(max_length=2, null=True, blank=True)  # Allow blank values as well

    class Meta:
        unique_together = ('city', 'country', 'state')
        indexes = [
            models.Index(fields=['city', 'country', 'state']),  # Adding an index for frequent lookups
        ]
from django.db import models
from skills.models import Skills
from companies.models import Company
from locations.models import Locations

from jobportal_cron.models import AbstractBaseModel

class JobLocation(AbstractBaseModel):
    job = models.ForeignKey('jobs.Jobs', on_delete=models.CASCADE, db_column='jobs_id')
    location = models.ForeignKey('locations.Locations', on_delete=models.CASCADE, db_column='locations_id')
    remote = models.BooleanField(default=False)  # Additional field

    class Meta:
        unique_together = ('job', 'location')
        indexes = [
            models.Index(fields=['job', 'location']),  # Adding an index for faster lookups
        ]

        db_table = 'jobs_jobs_locations'  # Explicitly set the table name

class Jobs(AbstractBaseModel):
    title = models.CharField(max_length=300, null=False)
    category = models.CharField(max_length=300, default='')
    sub_category = models.CharField(max_length=300, default='')
    post = models.CharField(max_length=100000)
    required_skills = models.ManyToManyField(Skills, related_name='skills')
    required_experience = models.IntegerField(blank=True, null=True)
    location = models.ManyToManyField('locations.Locations', through='JobLocation', related_name='locations')
    company = models.ForeignKey(Company, on_delete=models.SET_NULL, related_name='company', null=True)
    date = models.DateTimeField(auto_now_add=True, blank=True)
    job_id = models.CharField(max_length=300, null=False, blank=False)
    job_url = models.CharField(max_length=1000, null=False, blank=False)
    reviewed = models.BooleanField(default=False)
    available = models.BooleanField(default=True)
    unavailable_date = models.DateTimeField(null=True, blank=True)
    remote = models.BooleanField(default=False)
    in_office = models.BooleanField(default=True)

    class Meta:
        unique_together = ('job_id', 'company')


class JobsStats(models.Model):
    total_available = models.IntegerField()
    total_unavailable = models.IntegerField()
    date = models.DateTimeField(auto_now_add=True, blank=True)
from django.db import models
from skills.models import Skills
from companies.models import Company
from locations.models import Locations

# Create your models here.
from jobportal_cron.models import AbstractBaseModel


class Jobs(AbstractBaseModel):
    title = models.CharField(max_length=300, null=False)
    category = models.CharField(max_length=300, default='')
    sub_category = models.CharField(max_length=300, default='')
    post = models.CharField(max_length=100000)
    required_skills = models.ManyToManyField(Skills, related_name='skills')
    required_experience = models.IntegerField(blank=True, null=True)
    location = models.ManyToManyField(Locations, related_name='locations')
    location_str = models.CharField(max_length=200, null=True)
    company = models.ForeignKey(Company, on_delete=models.SET_NULL, related_name='company', null=True)
    date = models.DateTimeField(auto_now_add=True, blank=True)
    job_id = models.CharField(max_length=300, null=False, blank=False)
    job_url = models.CharField(max_length=1000, null=False, blank=False)
    reviewed = models.BooleanField(default=False)
    available = models.BooleanField(default=True)
    unavailable_date = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ('job_id', 'company')


class JobsStats(models.Model):
    total_available = models.IntegerField()
    total_unavailable = models.IntegerField()
    date = models.DateTimeField(auto_now_add=True, blank=True)

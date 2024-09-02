from django.db import models
from talent_bridge_cron.storage_backends import PublicMediaStorage

# Create your models here.
from talent_bridge_cron.models import AbstractBaseModel


class Company(AbstractBaseModel):
    name = models.CharField(max_length=100, unique=True)
    career_page = models.URLField()
    job_openings = models.IntegerField(default=0)
    logo = models.ImageField(upload_to='companies/', storage=PublicMediaStorage)

    def save(self, *args, **kwargs):
        try:
            this = Company.objects.get(id=self.id)
            if this:
                this.logo.delete()
        except:
            pass
        super(Company, self).save(*args, **kwargs)

from django.db import models
from django.utils import timezone
from talent_bridge_cron.models import AbstractBaseModel
class ScrapyProject(AbstractBaseModel):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class ScrapySpider(AbstractBaseModel):
    project = models.ForeignKey(ScrapyProject, on_delete=models.CASCADE, related_name='spiders')
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class ScrapyJob(AbstractBaseModel):
    spider = models.ForeignKey(ScrapySpider, on_delete=models.CASCADE, related_name='jobs')
    job_id = models.CharField(max_length=100)
    status = models.CharField(max_length=100, choices=[
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('finished', 'Finished'),
        ('failed', 'Failed'),
    ])
    start_time = models.DateTimeField(default=timezone.now)
    end_time = models.DateTimeField(null=True, blank=True)
    log_file = models.FileField(upload_to='scrapy_logs/', null=True, blank=True)  # New field for log file


    def __str__(self):
        return f"{self.spider.name} - {self.job_id}"
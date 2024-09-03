# Create your models here.
from django.db import models

class ScrapyProject(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class ScrapySpider(models.Model):
    project = models.ForeignKey(ScrapyProject, on_delete=models.CASCADE, related_name='spiders')
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class ScrapyJob(models.Model):
    spider = models.ForeignKey(ScrapySpider, on_delete=models.CASCADE, related_name='jobs')
    job_id = models.CharField(max_length=100)
    status = models.CharField(max_length=100)
    start_time = models.DateTimeField(null=True, blank=True)
    end_time = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.spider.name} - {self.job_id}"
from celery import shared_task
import requests
from django.utils import timezone
from .models import ScrapyJob, ScrapySpider
import time

@shared_task
def run_spider(spider_id):
    spider = ScrapySpider.objects.get(id=spider_id)
    response = requests.post(f'http://localhost:6800/schedule.json', data={
        'project': spider.project.name,
        'spider': spider.name,
    })

    if response.status_code == 200:
        job_id = response.json().get('jobid')
        if job_id:
            job = ScrapyJob.objects.create(
                spider=spider,
                job_id=job_id,
                status='running',
                start_time=timezone.now()
            )

            # Polling the Scrapyd API to wait for job completion
            while True:
                job_status_response = requests.get(
                    f'http://localhost:6800/listjobs.json?project={spider.project.name}')
                if job_status_response.status_code == 200:
                    jobs_data = job_status_response.json()
                    
                    # Check if the job has moved to "finished" or "failed"
                    if any(j['id'] == job_id for j in jobs_data.get('finished', [])):
                        job.status = 'finished'
                        job.end_time = timezone.now()
                        job.save()
                        return f'Spider {spider.name} finished with job ID {job_id}'
                    
                    if any(j['id'] == job_id for j in jobs_data.get('failed', [])):
                        job.status = 'failed'
                        job.end_time = timezone.now()
                        job.save()
                        return f'Spider {spider.name} failed with job ID {job_id}'

                time.sleep(10)  # Sleep for 10 seconds before polling again
        else:
            return f'Failed to retrieve job ID for spider {spider.name}'
    else:
        return f'Failed to start spider {spider.name}'
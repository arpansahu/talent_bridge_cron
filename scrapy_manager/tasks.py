from celery import shared_task
import requests
from django.utils import timezone
from .models import ScrapyJob, ScrapySpider
import logging
import uuid

logger = logging.getLogger(__name__)

@shared_task
def run_spider(spider_id):
    try:
        # Retrieve the spider instance
        spider = ScrapySpider.objects.get(id=spider_id)
        logger.info(f"Starting spider: {spider.name} with spider_id: {spider_id}")

        # Generate a random UUID for the job
        job_id = str(uuid.uuid4())

        # Schedule the spider with the generated UUID
        response = requests.post(f'http://localhost:6800/schedule.json', data={
            'project': spider.project.name,
            'spider': spider.name,
            'job_id': job_id  # Pass the generated UUID here
        })

        if response.status_code == 200:
            # Check if the job ID was returned by the scheduling request
            response_job_id = response.json().get('jobid')
            if response_job_id:
                # Create a ScrapyJob entry in the database with the job ID and status
                job = ScrapyJob.objects.create(
                    spider=spider,
                    job_id=job_id,  # Use the generated UUID here
                    status='running',
                    start_time=timezone.now()
                )
                logger.info(f"ScrapyJob created with job_id: {job_id}")
                
                return f'Spider {spider.name} started with job ID {job_id}'

            else:
                logger.error(f"Failed to retrieve job ID for spider {spider.name}")
                return f'Failed to retrieve job ID for spider {spider.name}'
        else:
            logger.error(f"Failed to start spider {spider.name}. Response status: {response.status_code}")
            return f'Failed to start spider {spider.name}'
    except Exception as e:
        logger.error(f"Unexpected error in run_spider task: {e}")
        raise
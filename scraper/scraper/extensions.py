# extensions.py
import logging
from scrapy import signals
from scrapy.exceptions import NotConfigured
from django.utils import timezone
from django.core.files.base import ContentFile
from scrapy_manager.models import ScrapyJob
from django.db import transaction
from asgiref.sync import sync_to_async
import os

logger = logging.getLogger(__name__)

class SaveCrawlStatsExtension:
    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("CRAWLERSAVESTATS_ENABLED"):
            raise NotConfigured

        ext = cls()
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    async def spider_closed(self, spider):
        try:
            await sync_to_async(self.update_job)(spider)
        except Exception as e:
            logger.error(f"Failed to update job: {e}")

    def update_job(self, spider):
        try:
            job_id = spider.job_id
            logger.info(f"Updating ScrapyJob Object for {job_id}")
            log_file = spider.settings.get('LOG_FILE')

            if job_id:
                try:
                    with open(log_file, 'r', encoding='utf-8') as new_log_file:
                        log_content = new_log_file.read()
                except Exception as e:
                    logger.error(f"Failed to read log file: {e}")
                    return

                with transaction.atomic():
                    job = ScrapyJob.objects.get(job_id=job_id)
                    
                    # Save log content to a file field in Django model
                    log_content_encoded = log_content.encode('utf-8')  # Ensure encoding is handled
                    job.log_file.save(f'{spider.name}_{timezone.now().strftime("%Y%m%d_%H%M%S")}_{job_id}.log', ContentFile(log_content_encoded))
                    job.end_time = timezone.now()  # Update end time
                    job.status = 'finished' if spider.crawler.stats.get_value('finish_reason') == 'finished' else 'failed'
                    job.save()
                    logger.info(f"Updated ScrapyJob Object {spider.crawler.stats.get_value('finish_reason')}")
                
                logger.info(f"Updated ScrapyJob Object Now its time to delete local log file {log_file}")
                os.remove(str(log_file))

            else:
                logger.info(f"Not Updated ScrapyJob Object Since job_id is not present, may be this spider is started manually.")
            
        except Exception as e:
            logger.error(f"Error updating ScrapyJob Object : {e} for {job_id}")
import os
import scrapy
import logging
from datetime import datetime
import boto3
from scrapy.utils.project import get_project_settings
from scrapy import signals
from tqdm import tqdm

class ProgressBarSpider(scrapy.Spider):
    name = 'progress_bar_spider'

    # Custom settings specific to this spider
    custom_settings = {
        "LOG_FILE": f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        "LOG_LEVEL": "DEBUG",  # Ensure logging level is set appropriately
    }

    start_urls = [
        'https://arpansahu.me',
        'https://clock-work.arpansahu.me',
        'https://third-eye.arpansahu.me',
        # Add more URLs as needed
    ]
    
    def __init__(self,job_id=None, *args, **kwargs):
        super(ProgressBarSpider, self).__init__(*args, **kwargs)
        self.job_id = job_id  # Set the job ID here
        self.logger.info(f"self.job_id: {self.job_id}")

        # Setup S3 client using settings from Scrapy and custom MinIO configurations
        # settings = get_project_settings()
        # self.s3_client = boto3.client(
        #     's3',
        #     aws_access_key_id=settings.get('AWS_ACCESS_KEY_ID'),
        #     aws_secret_access_key=settings.get('AWS_SECRET_ACCESS_KEY'),
        #     region_name='us-east-1',  # Use the region name, even though MinIO doesn't require it
        #     endpoint_url='https://minio.arpansahu.me'  # Custom endpoint URL for MinIO
        # )
        
        # # Store the bucket name and other settings for reuse
        # self.bucket_name = settings.get('AWS_STORAGE_BUCKET_NAME')
        # self.profile_name = "portfolio"  # Replace with the actual profile name
        # self.project_name = settings.get('PROJECT_NAME')  # Replace with the actual project name

        # Progress bar initialization
        self.progress_bar = tqdm(total=len(self.start_urls), desc='Processing Jobs', unit='job')

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ProgressBarSpider, cls).from_crawler(crawler, *args, **kwargs)
        
        # Access settings through the crawler instance
        log_file = crawler.settings.get('LOG_FILE')
        log_level = crawler.settings.get('LOG_LEVEL')

        # Set up logging
        spider.setup_logging(log_file, log_level)
        
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def setup_logging(self, log_file, log_level):
        logger = logging.getLogger()
        logger.setLevel(log_level)

        # File handler for logging to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

        # Console handler for logging to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

        # Clear existing handlers, if any
        if logger.hasHandlers():
            logger.handlers.clear()

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    def spider_closed(self, spider):
        # Close the progress bar
        self.progress_bar.close()

        # Retrieve the log filename from the settings
        # log_file = self.settings.get('LOG_FILE')

        # # Upload the log file to S3 after the spider closes
        # upload_successful = self.upload_log_to_s3(log_file)
        
        # if upload_successful:
        #     # Delete the local log file after successful upload
        #     try:
        #         # os.remove(log_file)
        #         self.logger.info("Successfully deleted the local log file: %s", log_file)

        #     except OSError as e:
        #         self.logger.error("Error deleting the log file: %s - %s", log_file, str(e))
        # else:
        #     self.logger.error("Failed to upload the log file to S3, so it was not deleted.")

    # def upload_log_to_s3(self, file_path):
    #     todays_date = datetime.now().strftime('%Y-%m-%d')
    #     s3_file_name = f"{self.profile_name}/{self.project_name}/scrapy_logs/{self.name}/{todays_date}/{os.path.basename(file_path)}"
        
    #     try:
    #         self.s3_client.upload_file(file_path, self.bucket_name, s3_file_name)
    #         self.logger.info(f"Successfully uploaded {file_path} to S3 as {s3_file_name}")
    #         return True
    #     except Exception as e:
    #         self.logger.error(f"Failed to upload log file to S3: {e}")
    #         return False

    def parse(self, response):
        self.logger.info(f"Processing: {response.url}")
        # Your parsing logic here
        
        # Simulate item extraction for demonstration purposes
        item = {'url': response.url}
        
        # Send the item to the item pipeline
        yield item

        # Update the progress bar after each URL is processed
        self.progress_bar.update(1)
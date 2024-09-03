import os
import scrapy
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime
import boto3
from scrapy.utils.project import get_project_settings
from scrapy import signals
import re
from jobs.models import Jobs, JobLocation
from asgiref.sync import sync_to_async
from tqdm import tqdm

class GoogleJobsSpider(scrapy.Spider):
    name = 'google_spider'
    allowed_domains = ['google.com']
    start_urls = ['https://www.google.com/about/careers/applications/jobs/results/']

    # Configure logging to output to a file
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'{name}_{current_time}.log'

    # Configure logging to output to a file
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        filename=log_filename,
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(GoogleJobsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def __init__(self, *args, **kwargs):
        super(GoogleJobsSpider, self).__init__(*args, **kwargs)
        self.company_name = "Google"  # Set the company name here

        # Setup S3 client using settings from Scrapy and custom MinIO configurations
        settings = get_project_settings()
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.get('AWS_SECRET_ACCESS_KEY'),
            region_name='us-east-1',  # Use the region name, even though MinIO doesn't require it
            endpoint_url='https://minio.arpansahu.me'  # Custom endpoint URL for MinIO
        )
        
        # Store the bucket name and other settings for reuse
        self.bucket_name = settings.get('AWS_STORAGE_BUCKET_NAME')
        self.profile_name = "portfolio"  # Replace with the actual profile name
        self.project_name = settings.get('PROJECT_NAME')  # Replace with the actual project name

        # Initialize the tqdm progress bar
        self.progress_bar = tqdm(total=0, desc='Processing Jobs', unit='job')

    def spider_closed(self, spider):
        # Close the progress bar
        self.progress_bar.close()

        # Upload the log file to S3 after the spider closes
        upload_successful = self.upload_log_to_s3(self.log_filename)
        
        if upload_successful:
            # Delete the local log file after successful upload
            try:
                os.remove(self.log_filename)
                spider.logger.info("Successfully deleted the local log file: %s", self.log_filename)
            except OSError as e:
                spider.logger.error("Error deleting the log file: %s - %s", self.log_filename, str(e))
        else:
            spider.logger.error("Failed to upload the log file to S3, so it was not deleted.")

    def upload_log_to_s3(self, file_path):
        s3_file_name = f"{self.profile_name}/{self.project_name}/scrapy_logs/{self.name}/{os.path.basename(file_path)}"
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_file_name)
            self.logger.info(f"Successfully uploaded {file_path} to S3 as {s3_file_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to upload log file to S3: {e}")
            return False

    async def parse(self, response):
        self.logger.info("Parsing jobs list from %s", response.url)
        # Collect all "Learn More" links
        job_links = response.xpath("//li[contains(@class, 'lLd3Je')]//a[contains(@class, 'WpHeLc')]/@href").getall()

        # job_links = job_links[:1]  # Only take the first link

        for link in job_links:
            full_link = response.urljoin(link)
            job_id = full_link.split('/')[-1].split('-')[0]

            self.logger.info("Found job link: %s", full_link)
            self.logger.info("Found job id: %s", job_id)

            # Check if the job is already in the database asynchronously
            job_exists = await sync_to_async(Jobs.objects.filter(job_id=job_id).exists)()

            if not job_exists:
                self.logger.info("New job found, scraping: %s", full_link)
                self.progress_bar.total += 1  # Increment the total count in the progress bar

                yield scrapy.Request(url=full_link, callback=self.parse_job_details)
            else:
                self.logger.info("Job already scraped, skipping: %s", full_link)

        # Handle pagination
        next_page = response.xpath('//a[contains(@class, "WpHeLc") and contains(@aria-label, "next page")]/@href').get()
        if next_page:
            self.logger.info("Navigating to next page: %s", next_page)
            yield response.follow(next_page, callback=self.parse)
        else:
            self.logger.info("No more pages to navigate.")

    def parse_job_details(self, response):
        self.logger.info("Parsing job details from %s", response.url)
        # Extract the raw title
        raw_title = response.xpath("//h2[contains(@class, 'p1N2lc')]/text()").get()
        
        # Split the title on commas
        title_parts = raw_title.split(',') if raw_title else []
        
        # Assign title, category, and sub_category based on the split parts
        title = title_parts[0].strip() if len(title_parts) > 0 else None
        category = title_parts[1].strip() if len(title_parts) > 1 else None
        sub_category = title_parts[2].strip() if len(title_parts) > 2 else None
        
        # Extract visible locations
        visible_locations = response.xpath("//span[contains(@class, 'pwO9Dc vo5qdf')]//span[contains(@class, 'r0wTof')]/text()").getall()
        
        # Extract additional locations from the note
        additional_locations_text = response.xpath("//span[contains(@class, 'MyVLbf')]/b/text()").get()
        
        # Split the additional locations by semicolons, if they exist
        additional_locations = [loc.strip() for loc in additional_locations_text.split(';')] if additional_locations_text else []
        
        before_filtering_list = visible_locations + additional_locations
        # Clean Unwanted Chars
        cleaned_locations = [loc.replace(';', '').replace('@', '').strip() for loc in before_filtering_list]

        all_locations = self.process_locations(cleaned_locations)
        
        # Clean up the locations set by removing any unwanted characters (if necessary)
        
        # Extract the job_id from the job_url
        job_url = response.url
        job_id = job_url.split('/')[-1].split('-')[0]
        
        # Extract specific sections with their HTML structure
        minimum_and_preferred_qualifications = response.xpath('//div[contains(@class, "KwJkGe")]').get()
        about_the_job = response.xpath('//div[contains(@class, "aG5W3")]').get()
        responsibilities = response.xpath('//div[contains(@class, "BDNOWe")]').get()
        
        # Combine the sections into a single HTML string
        post = (
            f"{minimum_and_preferred_qualifications or ''}"
            f"{about_the_job or ''}"
            f"{responsibilities or ''}"
        )
        
        self.logger.info("Scraped job: %s", title)

        # Yield the item with title, category, sub_category, company name, and post content
        yield {
            'title': title,
            'category': category,
            'sub_category': sub_category,
            'locations': list(all_locations),  # Convert the set back to a list for JSON serialization
            'job_url': job_url,  # Include the complete job URL
            'job_id': job_id,  # Extracted job ID
            'company': self.company_name,  # Set the company name
            'post': post,  # Constructed HTML post content with <b> and <br> tags
        }

    def process_locations(self, locations):
        city_states_and_sar = {
            # City States
            'Singapore': 'Singapore, Singapore',
            'Monaco': 'Monaco, Monaco',
            'Vatican City': 'Vatican City, Vatican City',
            'San Marino': 'San Marino, San Marino',
            'Luxembourg': 'Luxembourg, Luxembourg',

            # SAR
            'Hong Kong': 'Hong Kong, Hong Kong',
            'Macau': 'Macau, Macau'
        }

        processed_locations = []
        in_office_location = None
        remote_location = None
        index_to_remove = None

        # First pass to detect in-office and remote locations
        count = 0
        for loc in locations:
            if 'In-office locations:' in loc:
                in_office_location = loc.split(':')[-1].strip()
                index_to_remove = count
            elif 'Remote location:' in loc:
                remote_location = f"{loc.split(':')[-1].strip('.')}"
                index_to_remove = count

            count += 1  # Increment count

        # Remove the identified in-office and remote location entries from the list
        if index_to_remove is not None:
            locations = locations[0: index_to_remove] + locations[index_to_remove+1:]

        # When remote_location is found its False, as well as in_office_location = None & remote_location = None then also False. True when in_office_location
        default_remote = True if in_office_location else False

        # Second pass to process all locations
        for loc in locations:
            if loc.strip() == in_office_location:
                # If the location is the in-office location, it is not remote
                new_location = {'location': loc.strip(), 'remote': False}
                if new_location not in processed_locations:
                    processed_locations.append(new_location)
            elif remote_location and remote_location.endswith(loc.strip()):
                # If the location matches the specified remote location, it is remote
                new_location = {'location': f'Remote, Remote, {loc.strip()}', 'remote': True}

                if new_location not in processed_locations:
                    processed_locations.append(new_location)
            else:
                # All other locations use the default remote status
                if 'USA' in loc:
                    # Case 0: When San Diego, CA, USA here CA belongs to ISO2 code of CANADA, so better it ISO3 code is present remove the ISO2 code both cant exists together
                    # Remove any optional space, followed by a two-character word and a comma
                    cleaned_location = re.sub(r'\s\w{2},\s*', '', loc)
                    # Ensure there's exactly one space before 'USA'
                    cleaned_location = re.sub(r'\s*USA', ' USA', cleaned_location)
                    new_location = {'location': cleaned_location.strip(), 'remote': default_remote}
                    if new_location not in processed_locations:
                        processed_locations.append(new_location)

                elif 'UK' in loc:
                    # Case 1 : When ['London, UK'] UK is present is ISO2 code but its GB in the Database. Replace it with GB.
                    new_loc = loc.replace('UK', 'GB')
                    new_location = {'location': new_loc.strip(), 'remote': default_remote}

                    if new_location not in processed_locations:
                        processed_locations.append(new_location)

                elif loc in city_states_and_sar:
                    # Case 2 : When SAR or City States are present in one word itself.
                    new_location = {'location': city_states_and_sar[loc].strip(), 'remote': default_remote}
                    if new_location not in processed_locations:
                        processed_locations.append(new_location)

                elif ',' not in loc and default_remote == True:
                    # Case 3: When loc is a Country only and default_remote is True
                    new_location = {'location': f'Remote, Remote, {loc.strip()}', 'remote': default_remote}
                    if new_location not in processed_locations:
                        processed_locations.append(new_location)
                else:
                    new_location = {'location': loc.strip(), 'remote': default_remote}
                    if new_location not in processed_locations:
                        processed_locations.append(new_location)

        return processed_locations
import scrapy
from tqdm import tqdm

class ProgressBarSpider(scrapy.Spider):
    name = "progress_bar_spider"
    start_urls = [
        'https://arpansahu.me',
        'https://clock-work.arpansahu.me',
        'https://third-eye.arpansahu.me',
        # Add more URLs as needed
    ]

    def __init__(self, *args, **kwargs):
        super(ProgressBarSpider, self).__init__(*args, **kwargs)
        # Initialize the progress bar with the total number of start URLs
        self.progress_bar = tqdm(total=len(self.start_urls))

    def parse(self, response):
        # Your parsing logic here
        self.logger.info(f"Processing: {response.url}")
        
        # Simulate item extraction for demonstration purposes
        item = {'url': response.url}
        
        # Send the item to the item pipeline

        
        # Update the progress bar after each URL is processed
        self.progress_bar.update(1)



    def closed(self, reason):
        # Close the progress bar when the spider finishes
        self.progress_bar.close()
        self.logger.info("Spider closed: %s", reason)
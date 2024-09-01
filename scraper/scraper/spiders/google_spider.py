import scrapy

class GoogleJobsSpider(scrapy.Spider):
    name = 'google_spider'
    allowed_domains = ['google.com']
    start_urls = ['https://www.google.com/about/careers/applications/jobs/results/']

    def __init__(self, *args, **kwargs):
        super(GoogleJobsSpider, self).__init__(*args, **kwargs)
        self.company_name = "Google"  # Set the company name here

    def parse(self, response):
        # Collect all "Learn More" links
        job_links = response.xpath("//li[contains(@class, 'lLd3Je')]//a[contains(@class, 'WpHeLc')]/@href").getall()
        for link in job_links:
            full_link = response.urljoin(link)
            yield scrapy.Request(url=full_link, callback=self.parse_job_details)

    def parse_job_details(self, response):
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
        
        # Combine all locations into one list and then convert to a set to remove duplicates
        all_locations = set(visible_locations + additional_locations)
        
        # Clean up the locations set by removing any unwanted characters (if necessary)
        cleaned_locations = {loc.replace(';', '').replace('@', '').strip() for loc in all_locations}
        
        # Extract the job_id from the job_url
        job_url = response.url
        job_id = job_url.split('/')[-1].split('-')[0]
        
        # Extract specific sections with their HTML structure
        minimum_and_preferred_qualifications = response.xpath('//div[contains(@class, "KwJkGe")]').get()
        about_the_job = response.xpath('//div[contains(@class, "aG5W3")]').get()
        responsibilities = response.xpath('//div[contains(@class, "BDNOWe")]').get()
        
        # Combine the sections into a single HTML string with <b> tags after <h3> tags and <br> for line breaks
        post = (
            f"{minimum_and_preferred_qualifications or ''}"
            f"{about_the_job or ''}"
            f"{responsibilities or ''}"
        )
        
        # Yield the item with title, category, sub_category, company name, and post content
        yield {
            'title': title,
            'category': category,
            'sub_category': sub_category,
            'locations': list(cleaned_locations),  # Convert the set back to a list for JSON serialization
            'job_url': job_url,  # Include the complete job URL
            'job_id': job_id,  # Extracted job ID
            'company': self.company_name,  # Set the company name
            'post': post,  # Constructed HTML post content with <b> and <br> tags
        }
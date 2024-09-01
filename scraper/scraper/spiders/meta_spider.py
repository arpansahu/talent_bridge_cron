import sqlite3
import time

import scrapy
from asgiref.sync import sync_to_async

from ..items import Job
from jobs.models import Jobs
from companies.models import Company


class MetaSpider(scrapy.Spider):
    name = "meta"
    allowed_domains = ["www.metacareers.com"]
    start_urls = ["https://www.metacareers.com/jobs/"]
    base_job_detailed_url = 'https://www.metacareers.com'
    company_name = 'Meta'

    def parse(self, response):
        time.sleep(5)
        print("Meta Scrapper Started")
        next_page_text = response.xpath('//*[@id="search_result"]/div/div[4]/div[2]/a/text()').extract()[-1]
        next_page_url = None
        if next_page_text == 'Next':
            next_page_url = response.xpath('//*[@id="search_result"]/div/div[4]/div[2]/a/@href').extract()[-1]

        jobs_on_current_page = response.xpath('//*[@id="search_result"]/div/div[3]')

        for job_no in range(1, len(jobs_on_current_page.css('a').getall()) + 1):
            item = Job()
            item['company'] = self.company_name
            item['job_url'] = \
                jobs_on_current_page.xpath(f'//*[@id="search_result"]/div/div[3]/div[{job_no}]/a/@href').extract()[0]
            item['job_id'] = item['job_url'].strip('/').split('/')[-1]
            item['title'] = jobs_on_current_page.xpath(
                '//*[@id="search_result"]/div/div[3]/div[1]/a/div/div/div/div[1]/text()').extract()[0]
            item['category'] = jobs_on_current_page.xpath(
                f'//*[@id="search_result"]/div/div[3]/div[{job_no}]/a/div/div/div/div[3]/div[2]/div[2]/div/div/text()').extract()[
                0]
            item['sub_category'] = response.xpath(
                f'//*[@id="search_result"]/div/div[3]/div[{job_no}]/a/div/div/div/div[3]/div[2]/div[3]/div[2]/div/div/text()').extract()[
                0]

            yield response.follow(self.base_job_detailed_url + item['job_url'], callback=self.parse_job_details,
                                  cb_kwargs=dict(item=item))

        if next_page_url:
            print("Moving to Next Page")
            yield response.follow("https://www.metacareers.com" + next_page_url, callback=self.parse)

    def parse_job_details(self, response, item):
        time.sleep(3)
        locations_array = []

        try:
            one_location = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div[3]/div/div/div[2]/div/span').extract()
            two_locations = response.xpath('//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div['
                                           '3]/div/div/div[2]/div/span[2]/a').extract()
            more_than_two = response.xpath('//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div['
                                           '3]/div/div/div[2]/div/div').extract()

            if one_location and not two_locations:
                location = response.xpath('//*[@id="careersContentContainer"]/div/div[2]/div/div/div['
                                          '2]/div/div[3]/div/div/div[2]/div/span/text()').extract()[0]
                locations_array.append(location)

            elif two_locations and one_location:
                location = response.xpath(
                    '//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div[3]/div/div/div[2]/div/span[1]/span/a/text()').extract()[
                    0]
                locations_array.append(location)

                location = response.xpath(
                    '//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div[3]/div/div/div[2]/div/span[2]/a/text()').extract()[
                    0]
                locations_array.append(location)

            elif more_than_two:
                location = response.xpath(
                    '//*[@id="careersContentContainer"]/div/div[2]/div/div/div[2]/div/div[3]/div/div/div[2]/div/div/span[1]/span/a/text()').extract()[
                    0]
                locations_array.append(location)

                locations = response.xpath('//*[@id="locations"]/span')
                for location in locations.css('a::text').getall():
                    locations_array.append(location)

        except Exception as e:
            print("No locations Found")
            breakpoint()
            print(e)

        job_post = ''

        try:
            job_description = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[1]/text()').extract()[0]

            job_post += job_description + '\n\n'

        except Exception as e:
            breakpoint()
            print(e)

        try:
            res_head = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[3]/div[1]/text()').extract()[
                0]
            job_post += res_head + '\n\n'
        except Exception as e:
            breakpoint()
            print(e)

        try:
            responsibilities = ''
            all_res = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[3]')
            for div_no in range(1, len(all_res.css('li').getall()) + 1):
                responsibilities += '* ' + \
                                    all_res.xpath('//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div['
                                                  f'1]/div[1]/div[3]/div[2]/div/ul/div[{div_no}]/li/div[3]/div/div/text('
                                                  ')').extract()[0] + '\n '

            job_post += responsibilities + '\n'

        except Exception as e:
            breakpoint()
            print(e)

        try:
            min_qual_head = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[4]/div[1]/text()').extract()[
                0]

            job_post += min_qual_head + '\n\n'

        except Exception as e:
            breakpoint()
            print(e)

        try:
            min_qualifications = ''
            all_min_qual = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[4]/div[2]')
            for div_no in range(1, len(all_min_qual.css('li').getall()) + 1):
                min_qualifications += '* ' + all_min_qual.xpath('//*[@id="careersContentContainer"]/div/div[3]/div['
                                                                f'2]/div/div/div[1]/div[1]/div[4]/div[2]/div/ul/div[{div_no}]/li/div['
                                                                '3]/div/div/text()').extract()[0] + '\n '
            job_post += min_qualifications + '\n'

        except Exception as e:
            breakpoint()
            print(e)

        try:

            pref_qual_head = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[5]/div[1]/text()').extract()[
                0]
            job_post + pref_qual_head + '\n\n'

        except Exception as e:
            breakpoint()
            print(e)

        try:
            pref_qualifications = ''
            all_pref_qual = response.xpath(
                '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[5]/div[2]/div')
            for div_no in range(1, len(all_pref_qual.css('li').getall()) + 1):
                pref_qualifications += '* ' + all_pref_qual.xpath(
                    f'//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[5]/div[2]/div/ul/div[{div_no}]/li/div[3]/div/div/text()').extract()[
                    0] + '\n'

            job_post += pref_qualifications + '\n'

        except Exception as e:
            breakpoint()
            print(e)

        about_meta_head = 'About Meta'

        job_post += about_meta_head + '\n\n'

        try:
            meta_desc = ''

            for div_no in range(1, len(response.xpath(
                    '//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[7]/div').extract()) + 1):
                meta_para = response.xpath(
                    f'//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[7]/div[{div_no}]/div/text()').extract()
                meta_anchor_text = response.xpath(
                    f'//*[@id="careersContentContainer"]/div/div[3]/div[2]/div/div/div[1]/div[1]/div[7]/div[{div_no}]/div/a/text()').extract()
                if meta_para:
                    meta_para = meta_para[0]

                    for meta_anchor in meta_anchor_text:
                        meta_para += meta_anchor

                    if meta_para == 'About Meta':
                        meta_desc += meta_para + '\n\n'
                    else:
                        meta_desc += meta_para + '\n'

            job_post += meta_desc

        except Exception as e:
            breakpoint()
            print(e)

        item['locations'] = locations_array
        item['post'] = job_post

        return item

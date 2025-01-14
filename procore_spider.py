# ./procore_spider.py
import scrapy
from crochet import setup
setup()

class ProcoreSpider(scrapy.Spider):
    name = "procore"
    state_code = "ca"  # Default state code
    stop_requested = False  # Flag to stop the spider
    page_number = 1

    seen_business_names = set()
    scraped_data = []
    consecutive_empty_count = 0  # Counter for consecutive empty rows

    custom_settings = {
        "CONCURRENT_REQUESTS": 20,
        "DOWNLOAD_DELAY": 0,
        "LOG_LEVEL": "ERROR",
    }

    def start_requests(self):
        self.base_url = f"https://www.procore.com/network/us/{self.state_code}?page="
        url = self.base_url + str(self.page_number)
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        if self.stop_requested:
            return

        business_divs = response.css("div.sc-eCstZk.MuiBox-root")

        if not business_divs:
            self.logger.info(f"No more businesses found on page {self.page_number}. Scraping finished.")
            return

        for business in business_divs:
            if self.stop_requested:
                return

            business_name = business.css('h2[data-test-id="business-name"] span::text').get()

            if not business_name or business_name in self.seen_business_names:
                continue

            self.seen_business_names.add(business_name)
            spans = business.css('span[data-test-id="item-text"]::text').getall()
            location = spans[0] if len(spans) > 0 else None
            company_type = spans[1] if len(spans) > 1 else None
            market_services = spans[2] if len(spans) > 2 else None
            trades_services = spans[3] if len(spans) > 3 else None
            detail_page_link = business.css('a::attr(href)').get()

            if detail_page_link:
                detail_page_link = response.urljoin(detail_page_link)
                yield scrapy.Request(
                    detail_page_link,
                    callback=self.parse_business_detail,
                    meta={
                        'business_name': business_name,
                        'location': location,
                        'company_type': company_type,
                        'market_services': market_services,
                        'trades_services': trades_services
                    }
                )

        # Increment the page number and proceed to the next page
        self.page_number += 1
        next_page_url = self.base_url + str(self.page_number)
        yield scrapy.Request(next_page_url, callback=self.parse)

    def parse_business_detail(self, response):
        if self.stop_requested:
            return

        # phone_number = response.css('div.StyledBox-core-11_26_0__sc-fgsy0p-0.fWuYyi p.MuiTypography-body1::text').get()
        phone_number = response.css('div.StyledBox-core-12_15_0__sc-fgsy0p-0.ksFsez p.MuiTypography-body1::text').get()

        row = {
            "Business Name": response.meta['business_name'],
            "Phone Number": phone_number,
            "Location": response.meta['location'],
            "Company Type": response.meta['company_type'],
            "Market and Services": response.meta['market_services'],
            "Trades and Services": response.meta['trades_services'],
        }

        # Check if the row is empty (all values are None)
        if all(value is None for value in row.values()):
            self.consecutive_empty_count += 1
            if 1 <= self.consecutive_empty_count <= 4:
                # Add a row with None values
                empty_row = {key: None for key in row.keys()}
                self.scraped_data.append(empty_row)
            elif self.consecutive_empty_count > 18:
                # Stop the spider
                self.stop_requested = True
                self.crawler.engine.close_spider(self, 'Encountered more than 8 consecutive empty rows.')
        else:
            # Reset the counter if a non-empty row is found
            self.consecutive_empty_count = 0
            self.scraped_data.append(row)

        yield row  # Yield the row for data export

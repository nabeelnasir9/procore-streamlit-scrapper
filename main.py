import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess


class ProcoreSpider(scrapy.Spider):
    name = "procore"
    base_url = "https://www.procore.com/network/us/ca?page="
    page_number = 1

    start_urls = [
        base_url + str(page_number),
    ]
    scraped_data = []
    seen_business_names = set()

    custom_settings = {
        "CONCURRENT_REQUESTS": 20,
        "DNS_RESOLVER": "scrapy.resolver.CachingHostnameResolver",
        "DOWNLOAD_DELAY": 0,
        "LOG_LEVEL": "INFO",
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_EXPIRATION_SECS": 3600,
    }

    def parse(self, response):
        business_divs = response.css("div.sc-eCstZk.MuiBox-root")

        for business in business_divs:
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

    def parse_business_detail(self, response):
        phone_number = response.css('div.StyledBox-core-11_26_0__sc-fgsy0p-0.fWuYyi p.MuiTypography-body1::text').get()
        business_name = response.meta['business_name']
        location = response.meta['location']
        company_type = response.meta['company_type']
        market_services = response.meta['market_services']
        trades_services = response.meta['trades_services']
        row = {
            "Business Name": business_name,
            "Phone Number": phone_number,
            "Location": location,
            "Company Type": company_type,
            "Market and Services": market_services,
            "Trades and Services": trades_services,
        }

        print("Scraped Row:", row)
        self.scraped_data.append(row)
        self.save_to_excel()
        if len(response.css("div.sc-eCstZk.MuiBox-root")) > 0:
            self.page_number += 1
            next_page_url = self.base_url + str(self.page_number)
            yield scrapy.Request(next_page_url, callback=self.parse)

    def save_to_excel(self):
        df = pd.DataFrame(self.scraped_data)
        df = df.drop_duplicates(subset="Business Name", keep="first")
        print("Saving data to procore_business_data.xlsx")
        df.to_excel("procore_business_data.xlsx", index=False)

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ProcoreSpider)
    process.start()

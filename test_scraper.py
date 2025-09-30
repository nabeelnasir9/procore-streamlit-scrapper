#!/usr/bin/env python3
"""
Quick test script to verify the scraper is working locally
"""
import scrapy
from scrapy.crawler import CrawlerProcess
from procore_spider import ProcoreSpider
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

print("=" * 80)
print("Testing Procore Scraper Locally")
print("=" * 80)

# Create a crawler process
process = CrawlerProcess({
    'LOG_LEVEL': 'INFO',
    'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'TWISTED_REACTOR': 'twisted.internet.selectreactor.SelectReactor',
    'CONCURRENT_REQUESTS': 5,  # Reduced for testing
    'DOWNLOAD_DELAY': 1,  # Be nice to the server
})

# Set the state code
ProcoreSpider.state_code = 'ca'
ProcoreSpider.stop_requested = False
ProcoreSpider.scraped_data = []

print(f"\n✓ Testing with state code: CA")
print(f"✓ Target URL: https://network.procore.com/us/ca?page=1")
print("\nStarting scraper... (Press Ctrl+C to stop)\n")

# Run the spider
process.crawl(ProcoreSpider)
process.start()

# Print results
print("\n" + "=" * 80)
print(f"Scraping Complete!")
print(f"Total businesses scraped: {len(ProcoreSpider.scraped_data)}")
print("=" * 80)

if ProcoreSpider.scraped_data:
    print("\nFirst 5 businesses:")
    for i, business in enumerate(ProcoreSpider.scraped_data[:5], 1):
        print(f"\n{i}. {business.get('Business Name', 'N/A')}")
        print(f"   Phone: {business.get('Phone Number', 'N/A')}")
        print(f"   Location: {business.get('Location', 'N/A')}")
        print(f"   Type: {business.get('Company Type', 'N/A')}")
else:
    print("\n⚠️  No data was scraped. Check the logs above for errors.")


"""
Simple scraper using requests instead of Scrapy
This should work better in Streamlit Cloud environment
"""
import requests
from bs4 import BeautifulSoup
import re
import logging

logger = logging.getLogger(__name__)

class SimpleProcoreScraper:
    def __init__(self, state_code='ca'):
        self.state_code = state_code.lower()
        self.base_url = f"https://network.procore.com/us/{self.state_code}"
        self.scraped_data = []
        self.consecutive_blank_count = 0
        self.stop_requested = False
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
    
    def extract_phone_from_json(self, html_text):
        """Extract phone number from JSON data in the page"""
        phone_pattern = r'"phone":\s*"([^"]+)"'
        matches = re.findall(phone_pattern, html_text)
        for match in matches:
            if match and ('+' in match or '(' in match):
                return match
        return "Not Available"
    
    def scrape_page(self, page_num=1):
        """Scrape a single page"""
        url = f"{self.base_url}?page={page_num}"
        print(f"[SCRAPER] Scraping {url}")
        logger.info(f"Scraping {url}")
        
        try:
            print(f"[SCRAPER] Making request to {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            print(f"[SCRAPER] Got response, status: {response.status_code}")
            
            # Look for business links
            business_links = re.findall(r'href="(/p/[^"]+)"', response.text)
            print(f"[SCRAPER] Found {len(business_links)} business links")
            logger.info(f"Found {len(business_links)} business links")
            
            for idx, link in enumerate(business_links[:40], 1):  # Process all 40 businesses per page
                print(f"[SCRAPER] Processing business {idx}/{min(40, len(business_links))}: {link}")
                full_url = f"https://network.procore.com{link}"
                try:
                    detail_response = requests.get(full_url, headers=self.headers, timeout=10)
                    detail_response.raise_for_status()
                    
                    # Extract business name from URL
                    business_name = link.split('/')[-1].replace('-', ' ').title()
                    
                    # Extract phone from JSON
                    phone = self.extract_phone_from_json(detail_response.text)
                    
                    # Extract business info from JSON
                    company_type = 'Not Available'
                    market_services = 'Not Available'
                    trades_services = 'Not Available'
                    
                    # Look for businessTypes in JSON
                    business_types_match = re.search(r'"businessTypes":\s*\[([^\]]+)\]', detail_response.text)
                    if business_types_match:
                        types = re.findall(r'"([^"]+)"', business_types_match.group(1))
                        if types:
                            company_type = ', '.join(types)
                    
                    # Look for constructionSectors (Market Services)
                    sectors_match = re.search(r'"constructionSectors":\s*\[([^\]]+)\]', detail_response.text)
                    if sectors_match:
                        sectors = re.findall(r'"([^"]+)"', sectors_match.group(1))
                        if sectors:
                            market_services = ', '.join(sectors)
                    
                    # Look for providedServices (Trades and Services)
                    services_match = re.search(r'"providedServices":\s*\[([^\]]+)\]', detail_response.text)
                    if services_match:
                        # Extract service names
                        service_names = re.findall(r'"name":\s*"([^"]+)"', services_match.group(1))
                        if service_names:
                            trades_services = ', '.join(service_names[:3])  # Limit to first 3
                    
                    business_data = {
                        'Business Name': business_name,
                        'Phone Number': phone,
                        'Location': self.state_code.upper(),
                        'Company Type': company_type,
                        'Market and Services': market_services,
                        'Trades and Services': trades_services,
                    }
                    
                    # Check if data is blank (phone number is the key field)
                    if phone == "Not Available" or not phone or phone.strip() == "":
                        self.consecutive_blank_count += 1
                        print(f"[SCRAPER] ⚠️ Blank data ({self.consecutive_blank_count}/10)")
                    else:
                        self.consecutive_blank_count = 0  # Reset counter on successful scrape
                    
                    self.scraped_data.append(business_data)
                    print(f"[SCRAPER] ✓ Scraped: {business_name} - {phone}")
                    logger.info(f"Scraped: {business_name} - {phone}")
                    
                    # Stop if too many consecutive blanks
                    if self.consecutive_blank_count >= 10:
                        print(f"[SCRAPER] 🛑 Stopping: 10 consecutive entries with blank data")
                        self.stop_requested = True
                        break
                    
                except Exception as e:
                    logger.error(f"Error scraping {full_url}: {e}")
                    continue
            
            return len(business_links)
        
        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}")
            return 0
    
    def scrape(self, max_pages=200):
        """Scrape multiple pages with smart stopping"""
        print(f"[SCRAPER] Starting scrape for state: {self.state_code.upper()}, max_pages: {max_pages}")
        
        for page in range(1, max_pages + 1):
            # Check if stop was requested
            if self.stop_requested:
                print(f"[SCRAPER] 🛑 Stop requested after {page-1} pages")
                break
                
            print(f"[SCRAPER] Scraping page {page}/{max_pages}")
            links_found = self.scrape_page(page)
            
            if links_found == 0:
                print(f"[SCRAPER] No links found on page {page}, stopping")
                break
            
            # Check if we hit the consecutive blank limit
            if self.stop_requested:
                print(f"[SCRAPER] Stopped due to consecutive blank entries")
                break
        
        print(f"[SCRAPER] Scraping complete! Total businesses: {len(self.scraped_data)}")
        print(f"[SCRAPER] Data saved: {len([d for d in self.scraped_data if d['Phone Number'] != 'Not Available'])} with valid phone numbers")
        return self.scraped_data


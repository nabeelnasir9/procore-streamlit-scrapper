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
        "TWISTED_REACTOR": "twisted.internet.selectreactor.SelectReactor",
    }

    def start_requests(self):
        self.base_url = f"https://network.procore.com/us/{self.state_code}?page="
        url = self.base_url + str(self.page_number)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        yield scrapy.Request(url, headers=headers, callback=self.parse)

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

            # Look for business links within this div
            business_links = business.css('a[href*="/p/"]')
            
            for link in business_links:
                # Get business name from the link text or data attributes
                business_name = link.css('::text').get()
                if not business_name:
                    business_name = link.css('::attr(data-track-click)').get()
                    if business_name and ',' in business_name:
                        business_name = business_name.split(',')[-1].strip()
                
                if not business_name or business_name in self.seen_business_names:
                    continue
                
                # Clean up business name
                business_name = business_name.strip()
                if not business_name or len(business_name) < 3:
                    continue
                
                self.seen_business_names.add(business_name)
                
                # Get the detail page link
                detail_page_link = link.css('::attr(href)').get()
                
                # Try to extract additional info from the link's parent elements
                parent_div = link.xpath('..')
                all_text = parent_div.css('::text').getall()
                clean_text = [text.strip() for text in all_text if text.strip() and len(text.strip()) > 1]
                
                # Parse business information from the text
                location = None
                company_type = None
                market_services = None
                trades_services = None
                
                # Look for patterns in the text to extract business information
                for text in clean_text:
                    # Look for location patterns (state names, cities)
                    if any(loc in text for loc in ['California', 'CA', 'Los Angeles', 'San Francisco', 'San Diego', 'Sacramento', 'Oakland', 'Fresno', 'Long Beach', 'Bakersfield', 'Anaheim', 'Santa Ana', 'Riverside', 'Stockton', 'Irvine', 'Chula Vista', 'Fremont', 'San Bernardino', 'Modesto', 'Fontana', 'Oxnard', 'Moreno Valley', 'Huntington Beach', 'Glendale', 'Santa Clarita', 'Garden Grove', 'Oceanside', 'Rancho Cucamonga', 'Santa Rosa', 'Ontario', 'Lancaster', 'Elk Grove', 'Palmdale', 'Corona', 'Salinas', 'Pomona', 'Hayward', 'Escondido', 'Torrance', 'Sunnyvale', 'Orange', 'Fullerton', 'Pasadena', 'Thousand Oaks', 'Visalia', 'Simi Valley', 'Concord', 'Roseville', 'Santa Clara', 'Vallejo', 'Victorville', 'El Monte', 'Berkeley', 'Downey', 'Costa Mesa', 'Inglewood', 'Ventura', 'West Covina', 'Norwalk', 'Carlsbad', 'Fairfield', 'Richmond', 'Murrieta', 'Burbank', 'Antioch', 'Daly City', 'Santa Monica', 'Temecula', 'Clovis', 'Compton', 'Jurupa Valley', 'Vista', 'South Gate', 'Mission Viejo', 'Vacaville', 'Carson', 'Hesperia', 'Santa Barbara', 'Redding', 'Santa Cruz', 'Chico', 'Newport Beach', 'San Leandro', 'Hawthorne', 'Citrus Heights', 'Tracy', 'Alhambra', 'Livermore', 'Buena Park', 'Lakewood', 'Merced', 'Hemet', 'Chino', 'Menifee', 'Lake Forest', 'Napa', 'Redwood City', 'Bellflower', 'Indio', 'Baldwin Park', 'Chino Hills', 'Mountain View', 'Alameda', 'Upland', 'Folsom', 'San Ramon', 'Pleasanton', 'Union City', 'Lynwood', 'Apple Valley', 'Redlands', 'Turlock', 'Perris', 'Manteca', 'Milpitas', 'Lodi', 'Madera', 'Glendora', 'Pittsburg', 'Camarillo', 'Hanford', 'San Luis Obispo', 'Huntington Park', 'La Mesa', 'Arcadia', 'Fountain Valley', 'Diamond Bar', 'Santee', 'Porterville', 'Colton', 'Covina', 'Rohnert Park', 'Yorba Linda', 'Pacifica', 'Rancho Cordova', 'Montebello', 'Lompoc', 'Hollister', 'San Gabriel', 'Brea', 'La Habra', 'San Bruno', 'Beverly Hills', 'Coachella', 'Morgan Hill', 'Seaside', 'Calexico', 'San Dimas', 'Culver City', 'Los Banos', 'Martinez', 'San Mateo', 'Cypress', 'La Puente', 'Palm Desert', 'Novato', 'San Jacinto', 'La Verne', 'Goleta', 'Tulare', 'Petaluma']):
                        location = text
                    # Look for company type patterns
                    elif text in ['General Contractor', 'Specialty Contractor', 'Consultant', 'Supplier', 'Architect', 'Engineer', 'Owner Real Estate Developer']:
                        company_type = text
                    # Look for market services patterns
                    elif text in ['Commercial', 'Healthcare', 'Industrial and Energy', 'Infrastructure', 'Institutional', 'Residential']:
                        market_services = text
                    # Look for trades and services patterns
                    elif any(trade in text for trade in ['Concrete', 'Demolition', 'Design and Engineering', 'Project Management', 'HVAC', 'Structural Steel', 'Communications', 'Electrical', 'Plumbing', 'Roofing', 'Masonry', 'Landscaping', 'Earthwork', 'Fire Suppression', 'Electronic Security', 'Signage', 'Brick Tiling', 'Rough Carpentry', 'Heating Ventilating and Air Conditioning HVAC']):
                        trades_services = text
                
                # Process this business
                if detail_page_link:
                    detail_page_link = response.urljoin(detail_page_link)
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                    }
                    yield scrapy.Request(
                        detail_page_link,
                        headers=headers,
                        callback=self.parse_business_detail,
                        meta={
                            'business_name': business_name,
                            'location': location,
                            'company_type': company_type,
                            'market_services': market_services,
                            'trades_services': trades_services
                        }
                    )
                else:
                    # If no detail page, just yield the basic info
                    row = {
                        "Business Name": business_name,
                        "Phone Number": "Not Available",
                        "Location": location,
                        "Company Type": company_type,
                        "Market and Services": market_services,
                        "Trades and Services": trades_services,
                    }
                    self.scraped_data.append(row)
                    yield row
                
                # Only process one business per div to avoid duplicates
                break

        # Increment the page number and proceed to the next page
        self.page_number += 1
        next_page_url = self.base_url + str(self.page_number)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        yield scrapy.Request(next_page_url, headers=headers, callback=self.parse)

    def parse_business_detail(self, response):
        if self.stop_requested:
            return

        # Try to extract phone number from JSON data first
        phone_number = None
        
        # Look for JSON data in script tags
        script_tags = response.css('script::text').getall()
        for script in script_tags:
            if 'phone' in script.lower() and ('"' in script or "'" in script):
                # Try to find phone number patterns in the script
                import re
                phone_patterns = [
                    r'"phone":\s*"([^"]+)"',
                    r"'phone':\s*'([^']+)'",
                    r'"phone":\s*"([^"]*\+[^"]*)"',
                    r"'phone':\s*'([^']*\+[^']*)'",
                ]
                
                for pattern in phone_patterns:
                    matches = re.findall(pattern, script)
                    for match in matches:
                        if match and ('+' in match or '(' in match or match.replace('-', '').replace(' ', '').isdigit()):
                            phone_number = match.strip()
                            break
                    if phone_number:
                        break
                if phone_number:
                    break
        
        # If no phone number found in JSON, try HTML selectors
        if not phone_number or not phone_number.strip():
            phone_selectors = [
                'p.MuiTypography-body1::text',
                'div[class*="jss"] p.MuiTypography-body1::text',
                'p[class*="MuiTypography-body1"]::text',
                'div[class*="sc-"] p[class*="MuiTypography"]::text',
                'a[href^="tel:"]::text',
                'a[href^="tel:"]::attr(href)',
                '[data-test-id*="phone"]::text',
                '[data-test-id*="contact"]::text',
            ]
            
            for selector in phone_selectors:
                phone_number = response.css(selector).get()
                if phone_number and phone_number.strip():
                    phone_number = phone_number.strip()
                    if phone_number.startswith('tel:'):
                        phone_number = phone_number[4:]
                    break
        
        # If still no phone number found, try XPath approach
        if not phone_number or not phone_number.strip():
            phone_xpath_selectors = [
                '//p[contains(@class, "MuiTypography-body1")]/text()',
                '//div[contains(span, "Phone")]/p/text()',
                '//span[contains(text(), "Phone")]/following-sibling::p/text()',
                '//p[contains(text(), "+")]/text()',
                '//p[contains(text(), "(") and contains(text(), ")")]/text()',
            ]
            
            for xpath_selector in phone_xpath_selectors:
                phone_candidates = response.xpath(xpath_selector).getall()
                for candidate in phone_candidates:
                    candidate = candidate.strip()
                    if candidate and (candidate.startswith('+') or 
                                   ('(' in candidate and ')' in candidate) or
                                   (candidate.replace('-', '').replace(' ', '').replace('(', '').replace(')', '').isdigit() and len(candidate.replace('-', '').replace(' ', '').replace('(', '').replace(')', '')) >= 10)):
                        phone_number = candidate
                        break
                if phone_number:
                    break
        
        # If no phone number found, set as "Not Available"
        if not phone_number or not phone_number.strip():
            phone_number = "Not Available"

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

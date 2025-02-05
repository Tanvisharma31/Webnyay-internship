import requests
from bs4 import BeautifulSoup
import csv
import os
import re
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import time
from datetime import datetime

class SEBIScraper:
    def __init__(self, cutoff_date=None):
        self.base_url = "https://www.sebi.gov.in"
        self.attachdocs_base = "https://www.sebi.gov.in/sebi_data/attachdocs/"
        self.cutoff_date = datetime.strptime(cutoff_date, "%Y-%m-%d") if cutoff_date else None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.folder_urls = {
            "Legal": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingLegal=yes&sid=1&ssid=2&smid=0",
            "Rules": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0",
            "Regulations": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=3&smid=0",
            "Advisory": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=96&smid=0",
            "Circulars": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
            "Master Circulars": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0",
            "Guidelines": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=85&smid=0",
            "Gazette Notification": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=82&smid=0"
        }

    def construct_paginated_url(self, base_url, page, items_per_page=10):
        """Construct proper paginated URL for SEBI website"""
        parsed_url = urlparse(base_url)
        query_params = parse_qs(parsed_url.query)
        
        # Add or update pagination parameters
        query_params.update({
            'start': [str((page - 1) * items_per_page)],
            'length': [str(items_per_page)],
            'bm': ['normal']  # Required parameter for pagination
        })
        
        # Reconstruct URL with updated parameters
        new_query = urlencode(query_params, doseq=True)
        return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}?{new_query}"

    def fix_pdf_url(self, pdf_url):
        """Fix PDF URL to ensure it's properly formatted"""
        if not pdf_url:
            return None
            
        if pdf_url.startswith('http'):
            return pdf_url
            
        if pdf_url.startswith('/sebi_data/attachdocs'):
            return urljoin(self.base_url, pdf_url)
            
        if pdf_url.startswith('sebi_data/attachdocs'):
            return urljoin(self.base_url, '/' + pdf_url)
            
        return urljoin(self.attachdocs_base, pdf_url)

    def extract_pdf_from_iframe(self, html_url):
        """Extract PDF URL from iframe in HTML page"""
        try:
            response = self.session.get(html_url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            iframe = soup.find('iframe')
            if iframe and 'src' in iframe.attrs:
                src = iframe['src']
                if 'file=' in src:
                    pdf_url = src.split('file=')[-1]
                    return self.fix_pdf_url(pdf_url)
                
                if src.endswith('.pdf'):
                    return self.fix_pdf_url(src)
            
            # Look for PDF links in the page
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$'))
            if pdf_links:
                return self.fix_pdf_url(pdf_links[0]['href'])
            
            # Additional pattern matching for PDF URLs
            matches = re.findall(r'(?:sebi_data/attachdocs/|/sebi_data/attachdocs/)[^"\']+\.pdf', response.text)
            if matches:
                return self.fix_pdf_url(matches[0])
                
            return None
        except Exception as e:
            print(f"Error extracting PDF from {html_url}: {e}")
            return None

    def parse_date(self, date_str):
        """Parse date string with multiple format support"""
        date_formats = [
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%b %d, %Y",
            "%B %d, %Y",
            "%Y"
        ]
        
        for fmt in date_formats:
            try:
                date = datetime.strptime(date_str.strip(), fmt)
                if len(date_str) == 4:  # Year only
                    date = date.replace(month=1, day=1)
                return date
            except ValueError:
                continue
        return None

    def is_after_cutoff(self, date_str):
        """Check if a date string is after the cutoff date"""
        if not self.cutoff_date:
            return True
            
        parsed_date = self.parse_date(date_str)
        if parsed_date:
            return parsed_date >= self.cutoff_date
            
        print(f"Warning: Could not parse date: {date_str}")
        return True  # Include if date parsing fails

    def get_total_pages(self, soup):
        """Extract total number of pages from response"""
        try:
            # Look for total records count
            info_div = soup.find('div', class_='dataTables_info')
            if info_div:
                text = info_div.get_text()
                match = re.search(r'of (\d+) entries', text)
                if match:
                    total_records = int(match.group(1))
                    return (total_records + 9) // 10  # 10 items per page, rounded up
                    
            # Fallback to pagination links
            pagination = soup.find('div', class_='dataTables_paginate')
            if pagination:
                last_page = pagination.find_all('a')[-1]
                if 'data-dt-idx' in last_page.attrs:
                    return int(last_page['data-dt-idx'])
                    
            return 1
        except Exception as e:
            print(f"Error getting total pages: {e}")
            return 1

    def download_pdf(self, pdf_url, folder_name, file_name):
        """Download PDF with retry mechanism and timestamp recording"""
        try:
            folder_path = os.path.join('downloaded_data', folder_name)
            os.makedirs(folder_path, exist_ok=True)

            file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '-', '_', '.'))
            file_path = os.path.join(folder_path, file_name)
            metadata_path = file_path + '.meta'
            
            if os.path.exists(file_path):
                print(f"File {file_name} already exists in {folder_name}. Skipping download.")
                return True

            pdf_url = self.fix_pdf_url(pdf_url)
            if not pdf_url:
                print(f"Invalid PDF URL for {file_name}")
                return False

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(pdf_url, timeout=30)
                    response.raise_for_status()
                    
                    content_type = response.headers.get('content-type', '').lower()
                    if 'application/pdf' in content_type or pdf_url.endswith('.pdf'):
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        
                        # Record download timestamp and metadata
                        download_time = datetime.now().isoformat()
                        with open(metadata_path, 'w', encoding='utf-8') as f:
                            f.write(f"Downloaded: {download_time}\nSource URL: {pdf_url}")
                            
                        print(f"Downloaded: {file_name} in {folder_name}")
                        return True
                    else:
                        print(f"Warning: URL does not point to a PDF: {pdf_url}")
                        return False
                        
                except requests.RequestException as e:
                    if attempt == max_retries - 1:
                        print(f"Failed to download {file_name} after {max_retries} attempts: {e}")
                        return False
                    time.sleep(2 ** attempt)
                    
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")
            return False

    def scrape_folder_page(self, url, folder_name, seen_pdfs, filings_writer):
        """Scrape a folder page including embedded PDFs with pagination support"""
        try:
            pdf_entries = []
            page = 1
            
            # Get first page to determine total pages
            first_page_url = self.construct_paginated_url(url, page)
            response = self.session.get(first_page_url, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            total_pages = self.get_total_pages(soup)
            
            while page <= total_pages:
                paginated_url = self.construct_paginated_url(url, page)
                print(f"Scraping page {page}/{total_pages} of {folder_name}")
                
                try:
                    response = self.session.get(paginated_url, timeout=30)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    table = soup.find('table', {'id': 'sample_1'})
                    if not table:
                        break
                        
                    rows = table.find_all('tr')[1:]  # Skip header row
                    if not rows:
                        break
                        
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) > 1:
                            issue_date = cols[0].get_text(strip=True)
                            
                            if not self.is_after_cutoff(issue_date):
                                continue
                                
                            pdf_name = cols[1].get_text(strip=True)
                            link = cols[1].find('a')
                            
                            if link and 'href' in link.attrs:
                                original_url = urljoin(self.base_url, link['href'])
                                
                                if original_url.endswith('.html'):
                                    pdf_url = self.extract_pdf_from_iframe(original_url)
                                    if pdf_url and pdf_url not in seen_pdfs:
                                        seen_pdfs.add(pdf_url)
                                        entry = [folder_name, pdf_name, issue_date, pdf_url]
                                        filings_writer.writerow(entry)
                                        pdf_entries.append(entry)
                                        
                                elif original_url.endswith('.pdf'):
                                    if original_url not in seen_pdfs:
                                        seen_pdfs.add(original_url)
                                        entry = [folder_name, pdf_name, issue_date, original_url]
                                        filings_writer.writerow(entry)
                                        pdf_entries.append(entry)
                    
                    page += 1
                    time.sleep(2)  # Increased delay between pages
                    
                except requests.RequestException as e:
                    print(f"Error fetching page {page} of {folder_name}: {e}")
                    time.sleep(5)  # Longer delay on error
                    continue
            
            return pdf_entries
            
        except Exception as e:
            print(f"Error scraping folder {folder_name}: {e}")
            return []

    def run(self):
        """Main method to run the scraper"""
        seen_pdfs = set()
        
        with open('pdf_links.csv', mode='w', newline='', encoding='utf-8') as file:
            filings_writer = csv.writer(file)
            filings_writer.writerow(["Folder", "PDF Name", "Issue Date", "PDF Link"])
            
            for folder_name, url in self.folder_urls.items():
                print(f"\nScraping {folder_name}...")
                pdf_entries = self.scrape_folder_page(url, folder_name, seen_pdfs, filings_writer)
                
                for entry in pdf_entries:
                    folder, pdf_name, issue_date, pdf_link = entry
                    file_name = f"{pdf_name}_{issue_date}.pdf"
                    self.download_pdf(pdf_link, folder, file_name)
                    time.sleep(2)  # Increased delay between downloads

if __name__ == "__main__":
    cutoff_date = "2023-01-01"  # Format: YYYY-MM-DD
    scraper = SEBIScraper(cutoff_date=cutoff_date)
    scraper.run()
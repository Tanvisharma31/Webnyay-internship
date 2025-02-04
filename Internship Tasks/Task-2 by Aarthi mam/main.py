import requests
from bs4 import BeautifulSoup
import csv
import os
import re
from urllib.parse import urljoin, urlparse
import time

class SEBIScraper:
    def __init__(self):
        self.base_url = "https://www.sebi.gov.in"
        self.attachdocs_base = "https://www.sebi.gov.in/sebi_data/attachdocs/"
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

    def fix_pdf_url(self, pdf_url):
        """Fix PDF URL to ensure it's properly formatted"""
        if not pdf_url:
            return None
            
        # If it's already a complete URL, return it
        if pdf_url.startswith('http'):
            return pdf_url
            
        # Handle relative paths starting with /sebi_data/attachdocs
        if pdf_url.startswith('/sebi_data/attachdocs'):
            return urljoin(self.base_url, pdf_url)
            
        # Handle paths that might be missing the leading slash
        if pdf_url.startswith('sebi_data/attachdocs'):
            return urljoin(self.base_url, '/' + pdf_url)
            
        return urljoin(self.attachdocs_base, pdf_url)

    def extract_pdf_from_iframe(self, html_url):
        """Extract PDF URL from iframe in HTML page"""
        try:
            response = self.session.get(html_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find iframe with PDF
            iframe = soup.find('iframe')
            if iframe and 'src' in iframe.attrs:
                src = iframe['src']
                # Extract PDF URL from the iframe src
                if 'file=' in src:
                    pdf_url = src.split('file=')[-1]
                    return self.fix_pdf_url(pdf_url)
                
                # Handle relative paths
                if src.endswith('.pdf'):
                    return self.fix_pdf_url(src)
            
            # Look for PDF in attachdocs using regex
            matches = re.findall(r'(?:sebi_data/attachdocs/|/sebi_data/attachdocs/)[^"\']+\.pdf', response.text)
            if matches:
                return self.fix_pdf_url(matches[0])
                
            return None
        except Exception as e:
            print(f"Error extracting PDF from {html_url}: {e}")
            return None

    def download_pdf(self, pdf_url, folder_name, file_name):
        """Download PDF with retry mechanism"""
        try:
            folder_path = os.path.join('downloaded_data', folder_name)
            os.makedirs(folder_path, exist_ok=True)

            # Clean filename of invalid characters
            file_name = "".join(c for c in file_name if c.isalnum() or c in (' ', '-', '_', '.'))
            file_path = os.path.join(folder_path, file_name)
            
            if os.path.exists(file_path):
                print(f"File {file_name} already exists in {folder_name}. Skipping download.")
                return True

            # Ensure PDF URL is properly formatted
            pdf_url = self.fix_pdf_url(pdf_url)
            if not pdf_url:
                print(f"Invalid PDF URL for {file_name}")
                return False

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(pdf_url, timeout=30)
                    response.raise_for_status()
                    
                    # Verify it's actually a PDF
                    content_type = response.headers.get('content-type', '').lower()
                    if 'application/pdf' in content_type or pdf_url.endswith('.pdf'):
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        print(f"Downloaded: {file_name} in {folder_name}")
                        return True
                    else:
                        print(f"Warning: URL does not point to a PDF: {pdf_url}")
                        return False
                        
                except requests.RequestException as e:
                    if attempt == max_retries - 1:
                        print(f"Failed to download {file_name} after {max_retries} attempts: {e}")
                        return False
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")
            return False

    def scrape_folder_page(self, url, folder_name, seen_pdfs, filings_writer):
        """Scrape a folder page including embedded PDFs"""
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            pdf_entries = []
            table = soup.find('table', {'id': 'sample_1'})
            
            if table:
                rows = table.find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) > 1:
                        issue_year = cols[0].get_text(strip=True)
                        pdf_name = cols[1].get_text(strip=True)
                        link = cols[1].find('a')
                        
                        if link and 'href' in link.attrs:
                            original_url = urljoin(self.base_url, link['href'])
                            
                            # Handle HTML pages with embedded PDFs
                            if original_url.endswith('.html'):
                                pdf_url = self.extract_pdf_from_iframe(original_url)
                                if pdf_url:
                                    if pdf_url not in seen_pdfs:
                                        seen_pdfs.add(pdf_url)
                                        filings_writer.writerow([folder_name, pdf_name, issue_year, pdf_url])
                                        pdf_entries.append([folder_name, pdf_name, issue_year, pdf_url])
                            # Handle direct PDF links
                            elif original_url.endswith('.pdf'):
                                if original_url not in seen_pdfs:
                                    seen_pdfs.add(original_url)
                                    filings_writer.writerow([folder_name, pdf_name, issue_year, original_url])
                                    pdf_entries.append([folder_name, pdf_name, issue_year, original_url])
            
            return pdf_entries
            
        except Exception as e:
            print(f"Error scraping folder {folder_name}: {e}")
            return []

    def run(self):
        """Main method to run the scraper"""
        seen_pdfs = set()
        
        # Create CSV file for storing PDF links
        with open('pdf_links.csv', mode='w', newline='', encoding='utf-8') as file:
            filings_writer = csv.writer(file)
            filings_writer.writerow(["Folder", "PDF Name", "Issue Year", "PDF Link"])
            
            # Scrape each folder
            for folder_name, url in self.folder_urls.items():
                print(f"\nScraping {folder_name}...")
                pdf_entries = self.scrape_folder_page(url, folder_name, seen_pdfs, filings_writer)
                
                # Download PDFs
                for entry in pdf_entries:
                    folder, pdf_name, issue_year, pdf_link = entry
                    file_name = f"{pdf_name}_{issue_year}.pdf"
                    self.download_pdf(pdf_link, folder, file_name)
                    time.sleep(1)  # Respect the server

if __name__ == "__main__":
    scraper = SEBIScraper()
    scraper.run()
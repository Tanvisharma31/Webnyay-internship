import requests
from bs4 import BeautifulSoup
import csv
import os
import logging

# List of folder URLs (update as per requirement)
folder_urls = {
    "Legal": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListingLegal=yes&sid=1&ssid=2&smid=0",
    "Rules": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0",
    "Regulations": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=3&smid=0",
    "Advisory": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=96&smid=0",
    "Circulars": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
    "Master Circulars": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=6&smid=0",
    "Guidelines": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=85&smid=0",
    "Gazette Notification": "https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=82&smid=0"
}

# Setting up logging configuration
logging.basicConfig(
    filename='process_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Function to download PDFs
def download_pdf(pdf_url, folder_name, file_name):
    try:
        # Create folder if it doesn't exist
        folder_path = os.path.join('downloaded_data', folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # Check if the file already exists
        file_path = os.path.join(folder_path, file_name)
        if os.path.exists(file_path):
            logging.info(f"File {file_name} already exists in {folder_name}. Skipping download.")
            return  # Skip download if file exists

        # Download the PDF
        response = requests.get(pdf_url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Downloaded: {file_name} in {folder_name}")
    except Exception as e:
        logging.error(f"Failed to download {file_name} in {folder_name}: {e}")

# Function to scrape PDF links from a folder page with pagination
def scrape_folder_page(url, folder_name, seen_pdfs):
    pdf_entries = []
    page = 0

    while True:
        logging.info(f"Scraping {folder_name}, Page {page + 1}...")
        params = {'page': page}  # Adjust the pagination parameter if required
        response = requests.get(url, params=params)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Locate table containing the rules and their links
        table = soup.find('table', {'id': 'sample_1'})  # Target the table with ID 'sample_1'
        if table:
            rows = table.find_all('tr')[1:]  # Skip header row
            if not rows:
                break  # Stop if there are no rows (end of pagination)
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 1:
                    pdf_name = cols[1].get_text(strip=True)
                    # Find the anchor tag within the second column
                    pdf_link = cols[1].find('a')['href']
                    issue_year = cols[0].get_text(strip=True)

                    # Construct the full URL if the link is relative
                    if not pdf_link.startswith('http'):
                        pdf_link = 'https://www.sebi.gov.in' + pdf_link

                    # Skip duplicate PDFs based on URL
                    if pdf_link in seen_pdfs:
                        logging.info(f"Skipping duplicate PDF: {pdf_name} ({pdf_link})")
                        continue
                    seen_pdfs.add(pdf_link)

                    # Store the PDF entry
                    pdf_entries.append([folder_name, pdf_name, issue_year, pdf_link])

        page += 1  # Increment to move to the next page

    return pdf_entries

# Function to scrape all PDF links and save them to CSV
def scrape_and_save():
    seen_pdfs = set()  # Set to track seen PDF URLs
    with open('pdf_links.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Folder", "PDF Name", "Issue Year", "PDF Link"])
        
        # Loop through each folder URL
        for folder_name, url in folder_urls.items():
            pdf_entries = scrape_folder_page(url, folder_name, seen_pdfs)
            for entry in pdf_entries:
                writer.writerow(entry)

# Function to download PDFs from the CSV and save to corresponding folders
def download_pdfs_from_csv():
    downloaded_pdfs = set()  # Set to track downloaded PDFs
    
    with open('pdf_links.csv', mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        for row in reader:
            folder, pdf_name, issue_year, pdf_link = row
            
            # Skip if the PDF has already been downloaded
            if pdf_link in downloaded_pdfs:
                logging.info(f"Skipping already downloaded PDF: {pdf_name}")
                continue
            
            file_name = f"{pdf_name}_{issue_year}.pdf"
            download_pdf(pdf_link, folder, file_name)
            downloaded_pdfs.add(pdf_link)

# Scrape the PDFs and save the links to CSV
logging.info("Starting scraping process...")
scrape_and_save()

# Now download the PDFs from the scraped CSV file
logging.info("Starting download process...")
download_pdfs_from_csv()

logging.info("Process completed.")

import requests
from bs4 import BeautifulSoup
import csv
import os

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

# Function to download PDFs
def download_pdf(pdf_url, folder_name, file_name):
    try:
        # Create folder if it doesn't exist
        folder_path = os.path.join('downloaded_data', folder_name)
        os.makedirs(folder_path, exist_ok=True)

        # Check if the file already exists
        file_path = os.path.join(folder_path, file_name)
        if os.path.exists(file_path):
            print(f"File {file_name} already exists in {folder_name}. Skipping download.")
            return  # Skip download if file exists

        # Download the PDF
        response = requests.get(pdf_url)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {file_name} in {folder_name}")
    except Exception as e:
        print(f"Failed to download {file_name} in {folder_name}: {e}")

# Function to scrape PDF links from a folder page
def scrape_folder_page(url, folder_name, seen_pdfs):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # List to store PDF entries
    pdf_entries = []
    
    # Locate table containing the rules and their links
    table = soup.find('table', {'id': 'sample_1'})  # Target the table with ID 'sample_1'
    if table:
        rows = table.find_all('tr')[1:]  # Skip header row
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
                    continue
                seen_pdfs.add(pdf_link)

                # Store the PDF entry
                pdf_entries.append([folder_name, pdf_name, issue_year, pdf_link])
    
    return pdf_entries

# Function to scrape all PDF links and save them to CSV
def scrape_and_save():
    seen_pdfs = set()  # Set to track seen PDF URLs
    with open('pdf_links.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Folder", "PDF Name", "Issue Year", "PDF Link"])
        
        # Loop through each folder URL
        for folder_name, url in folder_urls.items():
            print(f"Scraping {folder_name}...")
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
                continue
            
            file_name = f"{pdf_name}_{issue_year}.pdf"
            download_pdf(pdf_link, folder, file_name)
            downloaded_pdfs.add(pdf_link)

# Scrape the PDFs and save the links to CSV
scrape_and_save()

# Now download the PDFs from the scraped CSV file
download_pdfs_from_csv()

import os
import pandas as pd
import pdfplumber
import requests
from msal import ConfidentialClientApplication
from dotenv import load_dotenv
import webbrowser
import logging
from typing import Dict, List, Optional
import time
import shutil  # Added for safe file operations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pdf_processor.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

# Configuration
class Config:
    APPLICATION_ID = os.getenv("APPLICATION_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    SCOPES = ["User.Read", "Files.ReadWrite.All"] 
    AUTHORITY = "https://login.microsoftonline.com/consumers/"
    PDF_FOLDER = os.path.expanduser(r"C:\Users\tanvi\OneDrive\Documents\pdfs")
    EXCEL_FILE = "new.xlsx"
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    BACKUP_FOLDER = os.path.join(PDF_FOLDER, "originals")  # Added for backup

class OneDriveClient:
    def __init__(self):
        self.access_token = None
        self._validate_config()

    def _validate_config(self):
        """Validate that all required environment variables are set."""
        if not all([Config.APPLICATION_ID, Config.CLIENT_SECRET]):
            raise ValueError("Missing required environment variables. Please check your .env file.")

    def authenticate(self) -> None:
        """Handle Microsoft Graph API authentication."""
        try:
            client = ConfidentialClientApplication(
                client_id=Config.APPLICATION_ID,
                authority=Config.AUTHORITY,
                client_credential=Config.CLIENT_SECRET
            )
            
            auth_request_url = client.get_authorization_request_url(Config.SCOPES)
            webbrowser.open(auth_request_url)
            
            auth_code = input("Enter the authorization code: ")
            token_response = client.acquire_token_by_authorization_code(
                code=auth_code,
                scopes=Config.SCOPES
            )
            
            if 'access_token' not in token_response:
                raise Exception(f"Failed to acquire token: {token_response.get('error_description')}")
            
            self.access_token = token_response['access_token']
            logging.info("Successfully authenticated with Microsoft Graph API")
            
        except Exception as e:
            logging.error(f"Authentication failed: {str(e)}")
            raise

    def upload_file(self, file_path: str) -> Optional[str]:
        """Upload file to OneDrive and return shareable link."""
        for attempt in range(Config.MAX_RETRIES):
            try:
                headers = {"Authorization": f"Bearer {self.access_token}"}
                
                # Upload file
                upload_url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{os.path.basename(file_path)}:/content"
                with open(file_path, "rb") as file:
                    response = requests.put(upload_url, headers=headers, data=file)
                response.raise_for_status()
                
                # Create shareable link
                file_id = response.json()['id']
                share_url = f"https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/createLink"
                link_response = requests.post(
                    share_url,
                    headers=headers,
                    json={"type": "view", "scope": "anonymous"}
                )
                link_response.raise_for_status()
                
                return link_response.json()['link']['webUrl']
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(Config.RETRY_DELAY)
                else:
                    logging.error(f"Failed to upload {file_path} after {Config.MAX_RETRIES} attempts")
                    return None

class PDFProcessor:
    def __init__(self, onedrive_client: OneDriveClient):
        self.onedrive_client = onedrive_client
        self.df = pd.read_excel(Config.EXCEL_FILE)
        self.client_names = set(self.df["Client Name"].str.lower().tolist())
        self._ensure_backup_folder()

    def _ensure_backup_folder(self):
        """Create backup folder if it doesn't exist."""
        if not os.path.exists(Config.BACKUP_FOLDER):
            os.makedirs(Config.BACKUP_FOLDER)

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to remove invalid characters."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename.strip()

    def extract_client_name(self, pdf_path: str) -> Optional[str]:
        """Extract client name from PDF using multiple methods."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = pdf.pages[0].extract_text()
                lines = [line.strip() for line in text.splitlines() if line.strip()]
                
                # Print first few lines for debugging
                logging.debug(f"First 5 lines of {pdf_path}:")
                for line in lines[:5]:
                    logging.debug(line)
                
                # Method 1: Look for specific markers
                markers = ["to:", "to,", "dear", "attention:", "attn:"]
                for i, line in enumerate(lines):
                    lower_line = line.lower()
                    if any(marker in lower_line for marker in markers):
                        next_line = lines[i + 1] if i + 1 < len(lines) else None
                        if next_line:
                            # Clean up the client name
                            clean_name = next_line.strip().replace(".", "").replace(",", "")
                            if clean_name.lower() in self.client_names:
                                return clean_name
                
                # Method 2: Check first few lines
                for line in lines[:5]:
                    clean_line = line.strip().replace(".", "").replace(",", "")
                    if clean_line.lower() in self.client_names:
                        return clean_line
                
                logging.warning(f"Could not extract client name from {pdf_path}")
                return None
                
        except Exception as e:
            logging.error(f"Error extracting name from {pdf_path}: {str(e)}")
            return None

    def rename_pdf(self, old_path: str, client_name: str) -> Optional[str]:
        """Rename PDF file with client name and return new path."""
        try:
            # Create sanitized filename
            new_filename = f"{self._sanitize_filename(client_name)}.pdf"
            new_path = os.path.join(Config.PDF_FOLDER, new_filename)
            
            # Backup original file
            backup_path = os.path.join(Config.BACKUP_FOLDER, os.path.basename(old_path))
            shutil.copy2(old_path, backup_path)
            
            # Rename file
            if os.path.exists(new_path):
                counter = 1
                while os.path.exists(new_path):
                    base_name = f"{self._sanitize_filename(client_name)}_{counter}.pdf"
                    new_path = os.path.join(Config.PDF_FOLDER, base_name)
                    counter += 1
            
            os.rename(old_path, new_path)
            logging.info(f"Renamed '{old_path}' to '{new_path}'")
            return new_path
            
        except Exception as e:
            logging.error(f"Error renaming file {old_path}: {str(e)}")
            return None

    def process_pdfs(self) -> None:
        """Main processing function."""
        processed_files = 0
        failed_files = 0
        urls: Dict[str, str] = {}

        try:
            # Process each PDF
            for filename in os.listdir(Config.PDF_FOLDER):
                if not filename.lower().endswith('.pdf'):
                    continue

                file_path = os.path.join(Config.PDF_FOLDER, filename)
                logging.info(f"Processing {filename}")

                # Extract and validate client name
                client_name = self.extract_client_name(file_path)
                if not client_name:
                    failed_files += 1
                    continue

                # Rename the PDF
                new_path = self.rename_pdf(file_path, client_name)
                if not new_path:
                    failed_files += 1
                    continue

                # Upload to OneDrive
                shareable_link = self.onedrive_client.upload_file(new_path)
                if shareable_link:
                    urls[client_name] = shareable_link
                    processed_files += 1
                else:
                    failed_files += 1

            # Update Excel file
            if urls:
                self.update_excel(urls)

            logging.info(f"Processing complete. Processed: {processed_files}, Failed: {failed_files}")

        except Exception as e:
            logging.error(f"Error in process_pdfs: {str(e)}")
            raise

    def update_excel(self, urls: Dict[str, str]) -> None:
        """Update Excel file with URLs."""
        try:
            # Create URL column if it doesn't exist
            if 'url' not in self.df.columns:
                self.df['url'] = ''

            # Update URLs
            for client_name, url in urls.items():
                mask = self.df['Client Name'].str.lower() == client_name.lower()
                self.df.loc[mask, 'url'] = url

            # Save Excel file
            self.df.to_excel(Config.EXCEL_FILE, index=False)
            logging.info("Excel file updated successfully")

        except Exception as e:
            logging.error(f"Error updating Excel file: {str(e)}")
            raise

def main():
    try:
        # Initialize OneDrive client and authenticate
        onedrive_client = OneDriveClient()
        onedrive_client.authenticate()

        # Initialize and run PDF processor
        processor = PDFProcessor(onedrive_client)
        processor.process_pdfs()

    except Exception as e:
        logging.error(f"Application error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
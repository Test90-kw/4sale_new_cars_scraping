import os
import json
import logging
import time
import ssl
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict  # Credentials loaded from a dictionary (service account format)
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Required scopes for Drive access
        self.service = None  # Placeholder for Drive API service object
        self.parent_folder_ids = [  # IDs of top-level folders to upload into
            '1VKQ2qnYbQsQOh29x9tnS9qtFnwLMK1tB',
            '1Z1ByZAquecUrUwlamftrwmvuOxPoxhOb'
        ]
        self.logger = logging.getLogger(__name__)  # Logger instance
        self.setup_logging()  # Set up logging to file and console
        self.max_retries = 3  # Max number of retries for upload/folder creation
        self.base_delay = 4  # Base wait time (in seconds) for retry backoff

    def setup_logging(self):
        """Configure logging to both console and file."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # Output to console
                logging.FileHandler("drive_upload.log")  # Output to log file
            ]
        )
        self.logger.setLevel(logging.INFO)  # Set logger level to INFO

    def authenticate(self):
        """Authenticate with Google Drive API using the provided credentials."""
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds, num_retries=3)  # Initialize Drive API client
            self.logger.info("Successfully authenticated with Google Drive")
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            raise

    def get_or_create_folder(self, folder_name, parent_folder_id):
        """
        Get the ID of an existing folder or create it under the specified parent.
        Retries on failure using exponential backoff.
        """
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Query for existing folder with given name under the parent
                query = (f"name='{folder_name}' and "
                        f"'{parent_folder_id}' in parents and "
                        f"mimeType='application/vnd.google-apps.folder' and "
                        f"trashed=false")
                
                results = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='files(id, name)'
                ).execute()
                
                files = results.get('files', [])
                if files:
                    self.logger.info(f"Found existing folder '{folder_name}' in parent {parent_folder_id}")
                    return files[0]['id']  # Return ID of existing folder
                
                # Folder not found, so create it
                file_metadata = {
                    'name': folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [parent_folder_id]
                }
                folder = self.service.files().create(
                    body=file_metadata,
                    fields='id'
                ).execute()
                
                self.logger.info(f"Created new folder '{folder_name}' in parent {parent_folder_id}")
                return folder.get('id')  # Return ID of new folder
                
            except HttpError as e:
                # Handle 404 specifically (parent folder not found)
                if e.resp.status == 404:
                    self.logger.error(f"Parent folder not found (ID: {parent_folder_id})")
                    return None
                # Retry on failure
                retry_count += 1
                if retry_count < self.max_retries:
                    delay = self.base_delay * (2 ** retry_count)  # Exponential backoff
                    self.logger.info(f"Retrying folder creation after {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to create/get folder after {self.max_retries} attempts")
                    raise
            except Exception as e:
                self.logger.error(f"Error in get_or_create_folder: {e}")
                raise

    def upload_file(self, file_name, folder_id):
        """
        Upload a file to a specified folder on Google Drive.
        Supports retry with exponential backoff in case of network or SSL errors.
        """
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Check if the file exists locally
                if not os.path.exists(file_name):
                    self.logger.error(f"File not found: {file_name}")
                    return None

                # Ensure a valid folder ID is provided
                if not folder_id:
                    self.logger.error(f"Invalid folder ID for file {file_name}")
                    return None

                file_metadata = {
                    'name': os.path.basename(file_name),  # Upload with original file name
                    'parents': [folder_id]
                }
                
                media = MediaFileUpload(
                    file_name,
                    resumable=True,
                    chunksize=1024*1024  # Upload in 1MB chunks
                )
                
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                self.logger.info(f"Successfully uploaded {file_name} to folder {folder_id}")
                return file.get('id')  # Return uploaded file ID
                
            except (ssl.SSLEOFError, HttpError) as e:
                # Retry on network or Drive API errors
                retry_count += 1
                if retry_count < self.max_retries:
                    delay = self.base_delay * (2 ** retry_count)
                    self.logger.info(f"Retrying upload after {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to upload file after {self.max_retries} attempts")
                    raise
            except Exception as e:
                self.logger.error(f"Error uploading file {file_name}: {str(e)}")
                raise

    def save_files(self, files):
        """
        Save a list of files to multiple parent folders on Google Drive.
        Automatically creates a dated subfolder (yesterday's date) inside each parent.
        """
        try:
            # Use yesterday's date as the folder name
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            for parent_folder_id in self.parent_folder_ids:
                # Create/get a dated folder inside each parent
                folder_id = self.get_or_create_folder(yesterday, parent_folder_id)
                if not folder_id:
                    self.logger.error(f"Skipping uploads to parent folder {parent_folder_id}")
                    continue
                
                for file_name in files:
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            # Try uploading each file
                            self.upload_file(file_name, folder_id)
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count == self.max_retries:
                                self.logger.error(f"Failed to upload {file_name} after {self.max_retries} attempts")
                            else:
                                delay = self.base_delay * (2 ** retry_count)
                                self.logger.info(f"Retrying upload of {file_name} (attempt {retry_count + 1}) after {delay} seconds")
                                time.sleep(delay)
            
            self.logger.info("Files upload process completed")
            
        except Exception as e:
            self.logger.error(f"Error in save_files: {str(e)}")
            raise

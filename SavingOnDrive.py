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
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.parent_folder_ids = [
            '1VKQ2qnYbQsQOh29x9tnS9qtFnwLMK1tB',
            '1Z1ByZAquecUrUwlamftrwmvuOxPoxhOb'
        ]
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.max_retries = 3
        self.base_delay = 4  # Base delay in seconds

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("drive_upload.log")
            ]
        )
        self.logger.setLevel(logging.INFO)

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds, num_retries=3)
            self.logger.info("Successfully authenticated with Google Drive")
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            raise

    def get_or_create_folder(self, folder_name, parent_folder_id):
        """Get or create folder in the specified parent folder."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Check if folder exists
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
                    return files[0]['id']
                
                # Create new folder
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
                return folder.get('id')
                
            except HttpError as e:
                if e.resp.status == 404:
                    self.logger.error(f"Parent folder not found (ID: {parent_folder_id})")
                    return None
                retry_count += 1
                if retry_count < self.max_retries:
                    delay = self.base_delay * (2 ** retry_count)
                    self.logger.info(f"Retrying folder creation after {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to create/get folder after {self.max_retries} attempts")
                    raise
            except Exception as e:
                self.logger.error(f"Error in get_or_create_folder: {e}")
                raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive with retries."""
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                if not os.path.exists(file_name):
                    self.logger.error(f"File not found: {file_name}")
                    return None

                if not folder_id:
                    self.logger.error(f"Invalid folder ID for file {file_name}")
                    return None

                file_metadata = {
                    'name': os.path.basename(file_name),
                    'parents': [folder_id]
                }
                
                media = MediaFileUpload(
                    file_name,
                    resumable=True,
                    chunksize=1024*1024  # 1MB chunks
                )
                
                file = self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                self.logger.info(f"Successfully uploaded {file_name} to folder {folder_id}")
                return file.get('id')
                
            except (ssl.SSLEOFError, HttpError) as e:
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
        """Save files to all valid Google Drive folders."""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            for parent_folder_id in self.parent_folder_ids:
                folder_id = self.get_or_create_folder(yesterday, parent_folder_id)
                if not folder_id:
                    self.logger.error(f"Skipping uploads to parent folder {parent_folder_id}")
                    continue
                
                for file_name in files:
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
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

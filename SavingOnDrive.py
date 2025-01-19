import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta


class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.drive_service = None
        self.parent_folder_id = '14ze0WPiqxGjNv4ekTxQnmVopSIuJIMym'  # Your specified parent folder ID
        self.yesterday_folder_id = None

    def authenticate(self):
        """Authenticate with Google Drive."""
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        
        credentials = Credentials.from_service_account_info(
            self.credentials_dict,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        self.drive_service = build('drive', 'v3', credentials=credentials)

    def get_or_create_yesterday_folder(self):
        """Get or create a folder with yesterday's date."""
        if self.yesterday_folder_id:
            return self.yesterday_folder_id

        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Check if folder already exists
        query = f"'{self.parent_folder_id}' in parents and name='{yesterday}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        existing_folders = results.get('files', [])
        
        if existing_folders:
            # Use existing folder
            self.yesterday_folder_id = existing_folders[0]['id']
            print(f"Using existing folder '{yesterday}' with ID: {self.yesterday_folder_id}")
            return self.yesterday_folder_id
        
        # Create new folder if it doesn't exist
        folder_metadata = {
            'name': yesterday,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.parent_folder_id]
        }
        folder = self.drive_service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()
        
        self.yesterday_folder_id = folder['id']
        print(f"Created new folder '{yesterday}' with ID: {self.yesterday_folder_id}")
        return self.yesterday_folder_id

    def upload_file(self, file_name):
        """Upload a file to the yesterday's date folder."""
        from googleapiclient.http import MediaFileUpload
        
        folder_id = self.get_or_create_yesterday_folder()
        
        file_metadata = {
            'name': file_name,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(
            file_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        try:
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File {file_name} uploaded successfully with ID: {file.get('id')}")
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")
            raise

    def save_files(self, files):
        """Save multiple files to the yesterday's date folder."""
        for file_name in files:
            self.upload_file(file_name)


# import os
# import json
# from google.oauth2.service_account import Credentials
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from datetime import datetime, timedelta


# class SavingOnDrive:
#     def __init__(self, credentials_dict):
#         self.credentials_dict = credentials_dict
#         self.scopes = ['https://www.googleapis.com/auth/drive']
#         self.service = None

#     def authenticate(self):
#         # Load credentials directly from the JSON content
#         creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
#         self.service = build('drive', 'v3', credentials=creds)

#     def create_folder(self, folder_name, parent_folder_id=None):
#         file_metadata = {
#             'name': folder_name,
#             'mimeType': 'application/vnd.google-apps.folder'
#         }
#         if parent_folder_id:
#             file_metadata['parents'] = [parent_folder_id]

#         folder = self.service.files().create(body=file_metadata, fields='id').execute()
#         return folder.get('id')

#     def upload_file(self, file_name, folder_id):
#         file_metadata = {'name': file_name, 'parents': [folder_id]}
#         media = MediaFileUpload(file_name, resumable=True)
#         file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#         return file.get('id')

#     def save_files(self, files):
#         parent_folder_id = '14ze0WPiqxGjNv4ekTxQnmVopSIuJIMym'  # Your specified parent folder ID
#         yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#         folder_id = self.create_folder(yesterday, parent_folder_id)
#         for file_name in files:
#             self.upload_file(file_name, folder_id)
#         print(f"Files uploaded successfully to folder '{yesterday}' on Google Drive.")

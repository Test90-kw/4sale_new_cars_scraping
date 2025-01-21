import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta


class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.current_folder_id = None  # Store the current folder ID
        
    def authenticate(self):
        creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
        self.service = build('drive', 'v3', credentials=creds)
    
    def create_folder(self, folder_name, parent_folder_id=None):
        # Check if we already have a folder ID
        if self.current_folder_id:
            return self.current_folder_id
            
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        folder = self.service.files().create(body=file_metadata, fields='id').execute()
        self.current_folder_id = folder.get('id')  # Store the folder ID
        return self.current_folder_id
    
    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_name, resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')
    
    def save_files(self, files):
        parent_folder_id = '14ze0WPiqxGjNv4ekTxQnmVopSIuJIMym'
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Create folder only if it doesn't exist
        folder_id = self.create_folder(yesterday, parent_folder_id)
        
        for file_name in files:
            self.upload_file(file_name, folder_id)
            
        if not self.current_folder_id:  # Only print first time
            print(f"Files uploaded successfully to folder '{yesterday}' on Google Drive.")

import pandas as pd
import json
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from CarScraper import CarScraper
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive
import os

# Allow nested event loops
nest_asyncio.apply()

class MainScraper:
    def __init__(self, url):
        self.url = url
        self.data = []
        self.brand_data = []
        self.chunk_size = 3
        self.excel_files = []

    async def process_brand_chunk(self, brand_chunk):
        """Process a chunk of brands and create their Excel files."""
        chunk_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        for brand_info in brand_chunk:
            brand_name = brand_info['brand'].replace(" ", "_")
            types = brand_info['types']
            self.brand_data.append({'Brand': brand_name})
            
            excel_file_name = f"{brand_name}.xlsx"
            sheets_written = False
            
            with pd.ExcelWriter(excel_file_name) as writer:
                for car_type in types:
                    type_name = car_type['title'].replace(" ", "_")
                    type_link = car_type['type_link']
                    
                    details_scraper = DetailsScraping(type_link)
                    try:
                        car_details = await details_scraper.get_car_details()
                    except TimeoutError:
                        print(f"Timeout error while scraping {type_name}. Skipping...")
                        continue
                    
                    if car_details:
                        # Filter for cars published yesterday
                        filtered_details = [
                            car for car in car_details 
                            if car.get('date_published') == yesterday
                        ]
                        
                        if filtered_details:  # Only create sheet if there's data from yesterday
                            df = pd.DataFrame(filtered_details)
                            df.to_excel(writer, sheet_name=type_name[:31], index=False)
                            sheets_written = True
                            print(f"Saved {len(filtered_details)} cars from {yesterday} for {type_name}")
                        else:
                            print(f"No cars from {yesterday} found for {type_name}")
                
                if not sheets_written:
                    pd.DataFrame([{'No Data': f'No car details available for {yesterday}'}]).to_excel(
                        writer, sheet_name="No_Data", index=False
                    )
            
            # Only add to chunk_files if the file was actually created
            if os.path.exists(excel_file_name) and os.path.getsize(excel_file_name) > 0:
                chunk_files.append(excel_file_name)
                print(f"Excel file created for {brand_name} with types from {yesterday}.")
            else:
                try:
                    os.remove(excel_file_name)  # Remove empty file
                    print(f"Removed empty file for {brand_name}")
                except:
                    pass
        
        return chunk_files

    async def upload_chunk_to_drive(self, files, drive_saver):
        """Upload a chunk of files to Google Drive."""
        try:
            drive_saver.save_files(files)
            print(f"Chunk of {len(files)} files uploaded successfully.")
            
            # Clean up local files after successful upload
            for file in files:
                try:
                    os.remove(file)
                    print(f"Deleted local file: {file}")
                except Exception as e:
                    print(f"Error deleting {file}: {e}")
        except Exception as e:
            print(f"Error uploading chunk to Drive: {e}")

    async def scrape_and_create_excel(self):
        # Setup Google Drive
        try:
            credentials_json = os.environ.get('NEW_CAR_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("NEW_CAR_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
        except Exception as e:
            print(f"Failed to setup Google Drive: {e}")
            return

        # Step 1: Scrape brands and types
        scraper = CarScraper(self.url)
        brand_and_types_data = await scraper.scrape_brands_and_types()

        # Step 2: Process brands in chunks
        for i in range(0, len(brand_and_types_data), self.chunk_size):
            chunk = brand_and_types_data[i:i + self.chunk_size]
            print(f"Processing chunk {i//self.chunk_size + 1}")
            
            # Create Excel files for the chunk
            chunk_files = await self.process_brand_chunk(chunk)
            
            # Upload the chunk to Drive if there are files to upload
            if chunk_files:
                await self.upload_chunk_to_drive(chunk_files, drive_saver)
            
            # Add delay between chunks
            if i + self.chunk_size < len(brand_and_types_data):
                await asyncio.sleep(5)  # 5 seconds delay between chunks

        # Create master brand list
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        master_excel_name = f"master_brand_list_{yesterday}.xlsx"
        brand_df = pd.DataFrame(self.brand_data)
        brand_df.to_excel(master_excel_name, sheet_name="Brands", index=False)
        
        # Upload master list
        drive_saver.save_files([master_excel_name])
        os.remove(master_excel_name)
        print(f"Master brand list for {yesterday} uploaded to Drive.")

if __name__ == "__main__":
    url = "https://www.q84sale.com/ar/automotive/new-cars-1"
    main_scraper = MainScraper(url)
    asyncio.run(main_scraper.scrape_and_create_excel())

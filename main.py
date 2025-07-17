# Import required libraries
import pandas as pd
import json
import asyncio
import nest_asyncio
import logging
import os
from pathlib import Path
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from CarScraper import CarScraper                # Custom class to scrape brand/type info
from DetailsScraper import DetailsScraping       # Custom class to scrape detailed car data
from SavingOnDrive import SavingOnDrive          # Custom class to handle Google Drive saving

# Allow nested event loops to support asyncio in environments like Jupyter or nested async calls
nest_asyncio.apply()

class MainScraper:
    def __init__(self, url):
        self.url = url                                  # Main page URL to scrape from
        self.data = []                                   # Reserved for overall scraped data (unused in current script)
        self.brand_data = []                             # Tracks processed brand names
        self.chunk_size = 3                              # Number of brands to process per chunk
        self.excel_files = []                            # List of all generated Excel files (unused in current script)
        self.logger = logging.getLogger(__name__)        # Logger instance
        self.setup_logging()                             # Set up logging config
        self.temp_dir = Path("temp_files")               # Temporary folder for storing Excel files
        self.temp_dir.mkdir(exist_ok=True)               # Create the temp directory if it doesn't exist
        self.upload_retries = 3                          # Number of times to retry uploading to Drive
        self.chunk_delay = 5                             # Delay between processing each chunk (seconds)

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),                # Output to console
                logging.FileHandler('scraper.log')      # Output to a log file
            ]
        )
        self.logger.setLevel(logging.INFO)              # Set log level

    async def process_brand_chunk(self, brand_chunk):
        """Process a chunk of brands and create their Excel files."""
        chunk_files = []                                 # List of Excel files created in this chunk

        for brand_info in brand_chunk:
            brand_name = brand_info['brand'].replace(" ", "_")  # Normalize brand name for file names
            types = brand_info['types']                          # List of car types for this brand
            
            all_car_details = []                                 # Store all car details under this brand

            # Loop through each car type under the brand
            for car_type in types:
                type_name = car_type['title'].replace(" ", "_")  # Normalize type name
                type_link = car_type['type_link']                # URL to scrape details from
                
                details_scraper = DetailsScraping(type_link)     # Instantiate the detail scraper
                try:
                    car_details = await details_scraper.get_car_details()  # Scrape car detail data
                    if car_details:
                        all_car_details.append({
                            'type_name': type_name,
                            'details': car_details
                        })
                except TimeoutError:
                    self.logger.error(f"Timeout error while scraping {type_name}. Skipping...")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing {type_name}: {str(e)}")
                    continue
            
            # Proceed only if there are valid car details
            if all_car_details:
                self.brand_data.append({'Brand': brand_name})  # Save brand info
                excel_file_name = self.temp_dir / f"{brand_name}.xlsx"  # Path to save Excel

                try:
                    # Write each car type's data in separate Excel sheets
                    with pd.ExcelWriter(excel_file_name) as writer:
                        for type_data in all_car_details:
                            df = pd.DataFrame(type_data['details'])
                            sheet_name = type_data['type_name'][:31]  # Sheet names max 31 chars
                            df.to_excel(writer, sheet_name=sheet_name, index=False)

                    chunk_files.append(str(excel_file_name))  # Add Excel file to chunk list
                    self.logger.info(f"Excel file created for {brand_name} with types")
                except Exception as e:
                    self.logger.error(f"Error creating Excel file for {brand_name}: {str(e)}")
            else:
                self.logger.info(f"No car details found for {brand_name}. Skipping Excel file creation.")
        
        return chunk_files

    async def upload_chunk_to_drive(self, files, drive_saver):
        """Upload a chunk of files to Google Drive with retries."""
        if not files:
            return
            
        for attempt in range(self.upload_retries):
            try:
                drive_saver.save_files(files)  # Attempt to upload files
                self.logger.info(f"Chunk of {len(files)} files uploaded successfully")
                
                # Clean up local files after upload
                for file in files:
                    try:
                        os.remove(file)
                        self.logger.info(f"Deleted local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error deleting {file}: {e}")
                break  # Exit retry loop on success
            except Exception as e:
                self.logger.error(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt < self.upload_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff before retry
                else:
                    self.logger.error("Max retries reached for upload")
    
    async def scrape_and_create_excel(self):
        """Main processing function."""
        # Step 0: Setup Google Drive credentials from environment
        try:
            credentials_json = os.environ.get('NEW_CAR_GCLOUD_KEY_JSON')
            if not credentials_json:
                raise EnvironmentError("NEW_CAR_GCLOUD_KEY_JSON environment variable not found")
            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        try:
            # Step 1: Scrape brands and their car types
            scraper = CarScraper(self.url)
            brand_and_types_data = await scraper.scrape_brands_and_types()

            # Step 2: Process scraped data in chunks
            for i in range(0, len(brand_and_types_data), self.chunk_size):
                chunk = brand_and_types_data[i:i + self.chunk_size]
                self.logger.info(f"Processing chunk {i//self.chunk_size + 1}")
                
                # Step 3: Create Excel files for the chunk
                chunk_files = await self.process_brand_chunk(chunk)
                
                # Step 4: Upload files to Google Drive
                if chunk_files:
                    await self.upload_chunk_to_drive(chunk_files, drive_saver)
                
                # Step 5: Optional delay before processing next chunk
                if i + self.chunk_size < len(brand_and_types_data):
                    self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                    await asyncio.sleep(self.chunk_delay)

        except Exception as e:
            self.logger.error(f"Error in scrape_and_create_excel: {e}")
        finally:
            # Step 6: Cleanup temporary directory
            try:
                for file in self.temp_dir.glob("*"):
                    file.unlink()
                self.temp_dir.rmdir()
                self.logger.info("Cleaned up temporary directory")
            except Exception as e:
                self.logger.error(f"Error cleaning up temp directory: {e}")


# Entry point for the script
if __name__ == "__main__":
    url = "https://www.q84sale.com/ar/automotive/new-cars-1"  # URL to start scraping from
    main_scraper = MainScraper(url)                           # Instantiate main scraper
    asyncio.run(main_scraper.scrape_and_create_excel())       # Run the main process asynchronously

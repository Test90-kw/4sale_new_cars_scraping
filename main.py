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
from CarScraper import CarScraper
from DetailsScraper import DetailsScraping
from SavingOnDrive import SavingOnDrive

# Allow nested event loops
nest_asyncio.apply()

class MainScraper:
    def __init__(self, url):
        self.url = url
        self.data = []
        self.brand_data = []
        self.chunk_size = 3
        self.excel_files = []
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.chunk_delay = 5  # Delay between chunks in seconds

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    async def process_brand_chunk(self, brand_chunk):
        """Process a chunk of brands and create their Excel files."""
        chunk_files = []
        for brand_info in brand_chunk:
            brand_name = brand_info['brand'].replace(" ", "_")
            types = brand_info['types']
            
            # Collect all car details for this brand first
            all_car_details = []
            for car_type in types:
                type_name = car_type['title'].replace(" ", "_")
                type_link = car_type['type_link']
                
                details_scraper = DetailsScraping(type_link)
                try:
                    car_details = await details_scraper.get_car_details()
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
            
            # Only create Excel file if we have actual car details
            if all_car_details:
                self.brand_data.append({'Brand': brand_name})
                excel_file_name = self.temp_dir / f"{brand_name}.xlsx"
                
                try:
                    with pd.ExcelWriter(excel_file_name) as writer:
                        for type_data in all_car_details:
                            df = pd.DataFrame(type_data['details'])
                            df.to_excel(writer, sheet_name=type_data['type_name'][:31], index=False)
                    
                    chunk_files.append(str(excel_file_name))
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
                drive_saver.save_files(files)
                self.logger.info(f"Chunk of {len(files)} files uploaded successfully")
                
                # Clean up local files after successful upload
                for file in files:
                    try:
                        os.remove(file)
                        self.logger.info(f"Deleted local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error deleting {file}: {e}")
                break
            except Exception as e:
                self.logger.error(f"Upload attempt {attempt + 1} failed: {e}")
                if attempt < self.upload_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error("Max retries reached for upload")
    
    async def scrape_and_create_excel(self):
        """Main processing function."""
        # Setup Google Drive
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
            # Step 1: Scrape brands and types
            scraper = CarScraper(self.url)
            brand_and_types_data = await scraper.scrape_brands_and_types()

            # Step 2: Process brands in chunks
            for i in range(0, len(brand_and_types_data), self.chunk_size):
                chunk = brand_and_types_data[i:i + self.chunk_size]
                self.logger.info(f"Processing chunk {i//self.chunk_size + 1}")
                
                # Create Excel files for the chunk
                chunk_files = await self.process_brand_chunk(chunk)
                
                # Upload the chunk to Drive
                if chunk_files:
                    await self.upload_chunk_to_drive(chunk_files, drive_saver)
                
                # Add delay between chunks
                if i + self.chunk_size < len(brand_and_types_data):
                    self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                    await asyncio.sleep(self.chunk_delay)

        except Exception as e:
            self.logger.error(f"Error in scrape_and_create_excel: {e}")
        finally:
            # Clean up temp directory
            try:
                for file in self.temp_dir.glob("*"):
                    file.unlink()
                self.temp_dir.rmdir()
                self.logger.info("Cleaned up temporary directory")
            except Exception as e:
                self.logger.error(f"Error cleaning up temp directory: {e}")


if __name__ == "__main__":
    url = "https://www.q84sale.com/ar/automotive/new-cars-1"
    main_scraper = MainScraper(url)
    asyncio.run(main_scraper.scrape_and_create_excel())


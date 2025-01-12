import pandas as pd
import json
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from CarScraper import CarScraper
from DetailsScraper import DetailsScraping

# Allow nested event loops (useful in Jupyter)
nest_asyncio.apply()

class MainScraper:
    def __init__(self, url):
        self.url = url
        self.data = []  # Data for each brand/type
        self.brand_data = []  # To hold brand-level info

    async def scrape_and_create_excel(self):
        # Step 1: Scrape brands and types
        scraper = CarScraper(self.url)
        brand_and_types_data = await scraper.scrape_brands_and_types()

        # Step 2: Loop through each brand and create an Excel file
        for brand_info in brand_and_types_data:
            brand_name = brand_info['brand'].replace(" ", "_")  # Excel-friendly naming
            types = brand_info['types']

            # Initialize data list for master brand sheet
            self.brand_data.append({'Brand': brand_name})

            # Create an Excel file for each brand
            excel_file_name = f"{brand_name}.xlsx"
            sheets_written = False
            with pd.ExcelWriter(excel_file_name) as writer:
                # Scrape and add each type to the brand’s Excel file
                for car_type in types:
                    type_name = car_type['title'].replace(" ", "_")
                    type_link = car_type['type_link']

                    # Scrape details for each type
                    details_scraper = DetailsScraping(type_link)
                    try:
                        car_details = await details_scraper.get_car_details()
                    except TimeoutError:
                        print(f"Timeout error while scraping {type_name}. Skipping...")
                        continue

                    # Convert scraped data to DataFrame and write to the sheet
                    if car_details:
                        df = pd.DataFrame(car_details)
                        df.to_excel(writer, sheet_name=type_name[:31], index=False)  # Sheet name max 31 chars
                        sheets_written = True

                # Add a default sheet if no data was written
                if not sheets_written:
                    pd.DataFrame([{'No Data': 'No car details available'}]).to_excel(
                        writer, sheet_name="No_Data", index=False
                    )

            print(f"Excel file created for {brand_name} with types.")

        # Optional: Create a master list of all brands
        master_excel_name = "master_brand_list.xlsx"
        brand_df = pd.DataFrame(self.brand_data)
        brand_df.to_excel(master_excel_name, sheet_name="Brands", index=False)
        print(f"Master brand list saved to {master_excel_name}.")

if __name__ == "__main__":
    url = "https://www.q84sale.com/ar/automotive/new-cars-1"
    main_scraper = MainScraper(url)
    asyncio.run(main_scraper.scrape_and_create_excel())

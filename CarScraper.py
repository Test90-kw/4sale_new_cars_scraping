import pandas as pd
import json
import asyncio
import nest_asyncio  # Allows running async loops within existing event loops (useful in Jupyter environments)
import re
import json
from playwright.async_api import async_playwright  # Async version of Playwright for web automation
from playwright._impl._errors import Error  # Used to catch navigation errors
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # Useful for date arithmetic
from DetailsScraper import DetailsScraping  # Custom module to scrape details for each type

# # Allow nested event loops (useful in Jupyter)
# nest_asyncio.apply()

class CarScraper:
    def __init__(self, url):
        self.url = url  # Main page URL to start scraping from
        self.base_url = "https://www.q84sale.com"  # Base domain used to resolve relative links
        self.data = []  # List to hold the final structured data

    async def scrape_brands_and_types(self):
        # Start an asynchronous Playwright session
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Launch Chromium browser in headless mode
            page = await browser.new_page()  # Open a new browser tab
            await page.goto(self.url)  # Navigate to the main URL

            # Select all anchor elements inside brand wrapper containers
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            for element in brand_elements:
                title = await element.get_attribute('title')  # Get brand title (e.g., "Toyota")
                brand_link = await element.get_attribute('href')  # Get link to that brand's page

                print(f"Brand: {title}, Link: {brand_link}")

                if brand_link:
                    # Construct full URL if the link is relative
                    full_brand_link = f"{self.base_url}{brand_link}" if brand_link.startswith('/') else brand_link

                    # Create a new tab to scrape that brand's types
                    new_page = await browser.new_page()
                    types = await self.scrape_types(new_page, full_brand_link)  # Scrape types for this brand
                    await new_page.close()  # Close tab after scraping

                    # Append brand and types info to self.data
                    self.data.append({
                        'brand': title,
                        'brand_link': full_brand_link,
                        'types': types
                    })

            await browser.close()  # Close the browser session
        return self.data  # Return all collected data

    async def scrape_types(self, page, brand_link):
        try:
            # Navigate to the brand page
            await page.goto(brand_link)
            # Wait until type elements are visible (to ensure full load)
            await page.wait_for_selector('.styles_itemWrapper__MTzPB a', timeout=5000)
        except (Error, TimeoutError) as e:
            # Handle cases where the page fails to load
            print(f"Failed to navigate to {brand_link}: {e}")
            return []

        # Select all type elements (sub-listings under each brand)
        type_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

        types_data = []  # Store scraped types
        for element in type_elements:
            title = await element.get_attribute('title')  # Get type title (e.g., "Land Cruiser")
            type_link = await element.get_attribute('href')  # Get the link to that type’s page

            # Construct full link if relative
            full_type_link = f"{self.base_url}{type_link}" if type_link and type_link.startswith('/') else type_link

            # Add the title and full type link to the types list
            types_data.append({'title': title, 'type_link': full_type_link})

            # Invoke detail scraping for each type link
            if type_link:
                DetailsScraping(full_type_link)  # This function is expected to handle its own scraping logic

        return types_data  # Return all types under the given brand

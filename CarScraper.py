import pandas as pd
import json
import asyncio
import nest_asyncio
import re
import json
from playwright.async_api import async_playwright
from playwright._impl._errors import Error
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from DetailsScraper import DetailsScraping

# # Allow nested event loops (useful in Jupyter)
# nest_asyncio.apply()

class CarScraper:
    def __init__(self, url):
        self.url = url
        self.base_url = "https://www.q84sale.com"
        self.data = []

    async def scrape_brands_and_types(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(self.url)

            # Get all brand links and titles
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            for element in brand_elements:
                title = await element.get_attribute('title')
                brand_link = await element.get_attribute('href')

                print(f"Brand: {title}, Link: {brand_link}")

                if brand_link:
                    full_brand_link = f"{self.base_url}{brand_link}" if brand_link.startswith('/') else brand_link

                    # Create a new page to scrape brand types
                    new_page = await browser.new_page()
                    types = await self.scrape_types(new_page, full_brand_link)
                    await new_page.close()

                    self.data.append({
                        'brand': title,
                        'brand_link': full_brand_link,
                        'types': types
                    })

            await browser.close()
        return self.data

    async def scrape_types(self, page, brand_link):
        try:
            await page.goto(brand_link)
            await page.wait_for_selector('.styles_itemWrapper__MTzPB a', timeout=5000)
        except (Error, TimeoutError) as e:
            print(f"Failed to navigate to {brand_link}: {e}")
            return []

        type_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

        types_data = []
        for element in type_elements:
            title = await element.get_attribute('title')
            type_link = await element.get_attribute('href')
            full_type_link = f"{self.base_url}{type_link}" if type_link and type_link.startswith('/') else type_link
            types_data.append({'title': title, 'type_link': full_type_link})

            # Call DetailsScraping for each type link
            if type_link:
                DetailsScraping(full_type_link)

        return types_data

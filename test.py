import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import json

async def scrape_olx():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # set up context with a realistic user agent
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        url = "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao?f=p"
        print(f"Buscando URL: {url}")
        
        response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Wait a bit just in case
        await page.wait_for_timeout(5000)
        
        content = await page.content()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # We also want to find the JSON-LD or Next.js state which often contains all ad data cleanly.
        # Let's search for script tag with id "__NEXT_DATA__"
        next_data = soup.find('script', id='__NEXT_DATA__')
        
        if next_data:
            print("Found __NEXT_DATA__ script!")
            try:
                data = json.loads(next_data.string)
                # It's deeply nested, usually: props.pageProps.ads
                ads = data.get('props', {}).get('pageProps', {}).get('ads', [])
                print(f"Found {len(ads)} ads in NEXT_DATA.")
                
                with open('ads.json', 'w', encoding='utf-8') as f:
                    json.dump(ads, f, ensure_ascii=False, indent=2)
                    
            except Exception as e:
                print(f"Error parsing NEXT_DATA: {e}")
        else:
            print("No __NEXT_DATA__ found. Saving HTML for inspection.")
                
        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_olx())

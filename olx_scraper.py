import asyncio
import sqlite3
import json
import requests
import os
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

DB_FILE = "olx_imoveis.db"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8744563469:AAFgKvhcPPSG-QWU19aWJGVZZAvswcd29JM")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "8427371764")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Tabela principal com suporte a múltiplos sites e tipos de anúncio
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS imoveis (
            id TEXT PRIMARY KEY,
            title TEXT,
            price TEXT,
            url TEXT,
            category TEXT,
            location TEXT,
            source_site TEXT DEFAULT 'olx',
            ad_type TEXT DEFAULT 'owner',
            date_added TIMESTAMP,
            notified BOOLEAN DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erro Telegram: {e}")
        return False

def save_new_imoveis(ads, source_site, ad_type):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    new_ads = []
    
    for ad in ads:
        list_id = str(ad.get("id"))
        
        cursor.execute("SELECT id FROM imoveis WHERE id = ?", (list_id,))
        if cursor.fetchone() is None:
            cursor.execute('''
                INSERT INTO imoveis (id, title, price, url, category, location, source_site, ad_type, date_added, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', (list_id, ad.get("title"), ad.get("price"), ad.get("url"), ad.get("category"), ad.get("location"), source_site, ad_type, datetime.now()))
            new_ads.append(ad)
            
    conn.commit()
    conn.close()
    return new_ads

def notify_new_ads():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, price, url, location, source_site, ad_type FROM imoveis WHERE notified = 0 LIMIT 20")
    unnotified_ads = cursor.fetchall()
    
    for ad in unnotified_ads:
        ad_id, title, price, url, location, source, ad_type = ad
        
        icon = "🏢" if ad_type == "competitor" else "🏠"
        header = "CONCORRÊNCIA" if ad_type == "competitor" else "NOVO PROPRIETÁRIO"
        
        message = f"{icon} <b>{header} ({source.upper()})</b>\n\n"
        message += f"<b>{title}</b>\n"
        message += f"💰 <b>{price}</b>\n"
        message += f"📍 {location}\n\n"
        message += f"🔗 <a href='{url}'>Ver anúncio</a>"
        
        if send_telegram_message(message):
            cursor.execute("UPDATE imoveis SET notified = 1 WHERE id = ?", (ad_id,))
            conn.commit()
    conn.close()

async def scrape_olx(page, base_url, ad_type):
    all_ads = []
    for p_num in range(1, 3):
        url = f"{base_url}&o={p_num}" if p_num > 1 else base_url
        print(f"Buscando OLX ({ad_type}): {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            next_data = soup.find('script', id='__NEXT_DATA__')
            if next_data:
                data = json.loads(next_data.string)
                ads_raw = data.get('props', {}).get('pageProps', {}).get('ads', [])
                for a in ads_raw:
                    all_ads.append({
                        "id": a.get("listId"),
                        "title": a.get("subject"),
                        "price": a.get("price", "Sob consulta"),
                        "url": a.get("url"),
                        "location": a.get("location", "S. Sebastião"),
                        "category": a.get("category", "Imóvel")
                    })
            else: break
        except Exception as e:
            print(f"Erro OLX: {e}")
            break
    return all_ads

async def scrape_riviera(page):
    url = "https://www.rivieraimoveis.com/imoveis/venda/sao-sebastiao-sp"
    print(f"Buscando RIVIERA: {url}")
    ads = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        cards = soup.select('article.c49-property-card')
        for card in cards:
            link_tag = card.select_one('a.c49btn-details')
            title_tag = card.select_one('.c49-property-card_title')
            price_tag = card.select_one('.c49-property-card_rent-price')
            loc_tag = card.select_one('.c49-property-card_address')
            
            if link_tag and title_tag:
                href = link_tag.get('href')
                if not href.startswith('http'): href = "https://www.rivieraimoveis.com" + href
                # Riviera ID can be the last part of URL or a hash
                ad_id = "riv-" + href.split('/')[-1].split('?')[0]
                ads.append({
                    "id": ad_id,
                    "title": title_tag.get_text(strip=True),
                    "price": price_tag.get_text(strip=True) if price_tag else "Consulte",
                    "url": href,
                    "location": loc_tag.get_text(strip=True) if loc_tag else "S. Sebastião",
                    "category": "Venda"
                })
    except Exception as e:
        print(f"Erro Riviera: {e}")
    return ads

async def main():
    init_db()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()
        
        # 1. OLX - Donos
        ads_olx_owners = await scrape_olx(page, "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao?f=p", "owner")
        save_new_imoveis(ads_olx_owners, "olx", "owner")
        
        # 2. OLX - Profissionais
        ads_olx_prof = await scrape_olx(page, "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao?f=c", "competitor")
        save_new_imoveis(ads_olx_prof, "olx", "competitor")
        
        # 3. Riviera Imóveis
        ads_riviera = await scrape_riviera(page)
        save_new_imoveis(ads_riviera, "riviera", "competitor")
        
        notify_new_ads()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

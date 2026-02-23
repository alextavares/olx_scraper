import asyncio
import sqlite3
import json
import requests
import os
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

DB_FILE = "olx_imoveis.db"
# Lendo as chaves do ambiente (Configurado la no GitHub Secrets)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    print("AVISO: Variáveis de ambiente TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID não encontradas.")
    # Fallback para as chaves locais (apenas para teste no PC do usuário)
    if not TELEGRAM_TOKEN: TELEGRAM_TOKEN = "8744563469:AAFgKvhcPPSG-QWU19aWJGVZZAvswcd29JM"
    if not TELEGRAM_CHAT_ID: TELEGRAM_CHAT_ID = "8427371764"
else:
    print("Sucesso: Chaves do Telegram carregadas do ambiente.")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS imoveis (
            id TEXT PRIMARY KEY,
            title TEXT,
            price TEXT,
            url TEXT,
            category TEXT,
            location TEXT,
            date_added TIMESTAMP,
            notified BOOLEAN DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def send_telegram_message(text):
    """Envia uma mensagem para o bot do Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erro ao enviar mensagem no Telegram: {e}")
        return False

def save_new_imoveis(ads):
    """
    Salva novos imóveis no banco de dados e retorna a lista dos que foram inseridos.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    new_ads = []
    
    for ad in ads:
        list_id = str(ad.get("listId"))
        
        # Check if already exists
        cursor.execute("SELECT id FROM imoveis WHERE id = ?", (list_id,))
        if cursor.fetchone() is None:
            # It's a new property!
            title = ad.get("subject", "Sem Título")
            price = ad.get("price", "Sem Preço")
            url = ad.get("url", "")
            category = ad.get("category", "")
            location = ad.get("location", "")
            
            cursor.execute('''
                INSERT INTO imoveis (id, title, price, url, category, location, date_added, notified)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            ''', (list_id, title, price, url, category, location, datetime.now()))
            
            new_ads.append(ad)
            
    conn.commit()
    conn.close()
    return new_ads

def notify_new_ads():
    """Busca no banco os anúncios não notificados e envia pro Telegram."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, title, price, url, location FROM imoveis WHERE notified = 0 LIMIT 10") # mandamos de 10 em 10 pra não floodar
    unnotified_ads = cursor.fetchall()
    
    for ad in unnotified_ads:
        ad_id, title, price, url, location = ad
        
        # Monta a mensagem bonita pro Telegram
        message = f"🚨 <b>NOVO IMÓVEL (Particular)</b>\n\n"
        message += f"🏠 <b>{title}</b>\n"
        message += f"💰 <b>{price}</b>\n"
        message += f"📍 {location}\n\n"
        message += f"🔗 <a href='{url}'>Ver anúncio na OLX</a>"
        
        # Envia e marca como notificado
        if send_telegram_message(message):
            cursor.execute("UPDATE imoveis SET notified = 1 WHERE id = ?", (ad_id,))
            conn.commit()
            
    conn.close()

async def scrape_olx_page(page, url):
    print(f"Buscando: {url}")
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(3000) # Give it 3 extra seconds to render scripts
    
    content = await page.content()
    soup = BeautifulSoup(content, 'html.parser')
    
    next_data = soup.find('script', id='__NEXT_DATA__')
    if next_data:
        try:
            data = json.loads(next_data.string)
            ads = data.get('props', {}).get('pageProps', {}).get('ads', [])
            return ads
        except Exception as e:
            print(f"Erro ao extrair JSON-LD: {e}")
            return []
    else:
        print("Data script __NEXT_DATA__ não encontrado. A OLX pode estar bloqueando a requisição.")
        return []

async def main():
    print("Iniciando OLX Scraper...")
    init_db()
    
    # URL para São Sebastião, buscando na categoria "Imóveis" apenas "Particulares" (f=p)
    base_url = "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        all_new_ads = []
        
        # Vamos raspar apenas as 2 primeiras páginas por páginação
        for page_num in range(1, 3):
            if page_num == 1:
                url = f"{base_url}?f=p"
            else:
                url = f"{base_url}?f=p&o={page_num}"
                
            ads = await scrape_olx_page(page, url)
            
            if not ads:
                print("Nenhum anúncio encontrado ou bloqueado. Parando raspagem.")
                break
                
            new_in_page = save_new_imoveis(ads)
            all_new_ads.extend(new_in_page)
            print(f"Página {page_num}: {len(ads)} anúncios lidos. {len(new_in_page)} eram NOVIDADE!")
            
            # Avisa o telegram dos novos na hora
            notify_new_ads()
        
        await browser.close()
        
        print(f"\nBusca concluída! {len(all_new_ads)} imóveis novos encontrados nesta rodada.")

if __name__ == "__main__":
    asyncio.run(main())

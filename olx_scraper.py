import asyncio
import sqlite3
import json
import requests
import os
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Novos módulos (IA e Mensageria)
from telegram_sender import TelegramSender
from ai_contact_logic import AIContactLogic

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
            notified BOOLEAN DEFAULT 0,
            contacted BOOLEAN DEFAULT 0
        )
    ''')
    
    # Migração automática de colunas antigas (caso o DB no GitHub esteja desatualizado)
    cursor.execute("PRAGMA table_info(imoveis)")
    columns = [column[1] for column in cursor.fetchall()]
    if "source_site" not in columns:
        cursor.execute("ALTER TABLE imoveis ADD COLUMN source_site TEXT DEFAULT 'olx'")
    if "ad_type" not in columns:
        cursor.execute("ALTER TABLE imoveis ADD COLUMN ad_type TEXT DEFAULT 'owner'")
    if "contacted" not in columns:
        cursor.execute("ALTER TABLE imoveis ADD COLUMN contacted BOOLEAN DEFAULT 0")
        
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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Verificando novos anúncios para notificar...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, price, url, location, source_site, ad_type FROM imoveis WHERE notified = 0 LIMIT 100")
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
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Notificações concluídas: {len(unnotified_ads)} processados.")
    conn.close()

async def scrape_olx(page, base_url, ad_type):
    all_ads = []
    print(f"Iniciando raspagem OLX: {ad_type}")
    for p_num in range(1, 3):
        url = f"{base_url}&o={p_num}" if p_num > 1 else base_url
        print(f"Buscando OLX ({ad_type}) [p{p_num}]: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            next_data = soup.find('script', id='__NEXT_DATA__')
            if next_data:
                data = json.loads(next_data.string)
                ads_raw = data.get('props', {}).get('pageProps', {}).get('ads', [])
                if not ads_raw: break
                for a in ads_raw:
                    all_ads.append({
                        "id": a.get("listId"),
                        "title": a.get("subject"),
                        "price": a.get("price", "Sob consulta"),
                        "url": a.get("url"),
                        "location": a.get("location", "S. Sebastião"),
                        "category": a.get("category", "Imóvel")
                    })
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] AVISO: __NEXT_DATA__ não encontrado na OLX ({ad_type}).")
                break
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro OLX ({ad_type}): {e}")
            break
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fim raspagem OLX ({ad_type}): {len(all_ads)} anúncios encontrados.")
    return all_ads

async def scrape_riviera(page):
    url = "https://www.rivieraimoveis.com/imobiliaria/venda/sao-sebastiao-sp/imoveis/364/1"
    print(f"Buscando RIVIERA: {url}")
    ads = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(10000) # Site pesado
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        cards = soup.select('article.c49-property-card')
        print(f"Riviera: {len(cards)} cards encontrados.")
        for card in cards:
            link_tag = card.select_one('a.c49btn-details')
            title_tag = card.select_one('h2') or card.select_one('.c49-property-card_title')
            price_tag = card.select_one('.c49-property-card_rent-price') or card.find(lambda tag: tag.name == 'div' and 'R$' in tag.text)
            loc_tag = card.select_one('.c49-property-card_address') or card.select_one('.c49-property-card_header div')
            
            if link_tag and (title_tag or card.select_one('.c49-property-card_title')):
                href = link_tag.get('href')
                if not href.startswith('http'): href = "https://www.rivieraimoveis.com" + href
                
                # ID mais robusto
                raw_id = href.split('/')[-1].split('?')[0]
                if not raw_id or raw_id == '1':
                    # Fallback para o penúltimo segmento se o último for 1 ou vazio
                    raw_id = href.split('/')[-2]
                
                ad_id = f"riv-{raw_id}"
                ads.append({
                    "id": ad_id,
                    "title": (title_tag or card.select_one('.c49-property-card_title')).get_text(strip=True),
                    "price": price_tag.get_text(strip=True) if price_tag else "Consulte",
                    "url": href,
                    "location": loc_tag.get_text(strip=True) if loc_tag else "São Sebastião",
                    "category": "Venda"
                })
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro Riviera: {e}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fim RIVIERA: {len(ads)} ads.")
    return ads

async def scrape_iz(page):
    url = "https://www.izimoveis.com.br/imoveis/a-venda/sao-sebastiao"
    print(f"Buscando IZ IMÓVEIS: {url}")
    ads = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        cards = soup.select('a.card-with-buttons')
        print(f"IZ: {len(cards)} cards encontrados.")
        for card in cards:
            href = card.get('href')
            if not href: continue
            if not href.startswith('http'): href = "https://www.izimoveis.com.br" + href
            
            title_tag = card.select_one('h2')
            price_tag = card.select_one('.card-with-buttons__value')
            
            if title_tag or card.select_one('.card-with-buttons__title'):
                real_title_tag = title_tag or card.select_one('.card-with-buttons__title')
                # ID limpo (sem query params)
                raw_id = href.split('/')[-1].split('?')[0]
                ad_id = f"iz-{raw_id}"
                ads.append({
                    "id": ad_id,
                    "title": real_title_tag.get_text(strip=True),
                    "price": price_tag.get_text(strip=True) if price_tag else "Consulte",
                    "url": href,
                    "location": "São Sebastião",
                    "category": "Venda"
                })
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro IZ: {e}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fim IZ: {len(ads)} ads.")
    return ads

async def scrape_tropical(page):
    url = "https://tropicalimobiliaria.com.br/comprar/sp/sao-sebastiao/pagina-1/"
    print(f"Buscando TROPICAL: {url}")
    ads = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(5000)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        cards = soup.select('a.link_resultado')
        print(f"Tropical: {len(cards)} cards encontrados.")
        for card in cards:
            href = card.get('href')
            if not href: continue
            
            title_tag = card.select_one('h3')
            price_tag = card.select_one('h5')
            loc_tag = card.select_one('.final_card')
            
            if title_tag:
                if not href.startswith('http'): 
                    href = "https://tropicalimobiliaria.com.br" + href
                # Extrair o ID/Ref do final da URL (geralmente tem um código)
                raw_id = href.split('/')[-1] or href.split('/')[-2]
                ad_id = f"trop-{raw_id}"
                ads.append({
                    "id": ad_id,
                    "title": title_tag.get_text(strip=True),
                    "price": price_tag.get_text(strip=True) if price_tag else "Consulte",
                    "url": href,
                    "location": loc_tag.get_text(strip=True) if loc_tag else "São Sebastião",
                    "category": "Venda"
                })
    except Exception as e:
        print(f"Erro Tropical: {e}")
    return ads

async def scrape_adimov(page):
    url = "https://www.adimov.com.br/imobiliaria/imoveis"
    print(f"Buscando ADIMOV: {url}")
    ads = []
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(8000)
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        cards = soup.select('article')
        print(f"Adimov: {len(cards)} cards encontrados.")
        for card in cards:
            link_tag = card.select_one('a.c49btn-details')
            title_tag = card.select_one('.c49-property-card_header h2')
            price_tag = card.select_one('.c49-property-card_price')
            
            if link_tag and (title_tag or card.select_one('h2')):
                real_title_tag = title_tag or card.select_one('h2')
                href = link_tag.get('href')
                if not href.startswith('http'): href = "https://www.adimov.com.br" + href
                raw_id = href.split('/')[-1].split('?')[0]
                ad_id = f"adi-{raw_id}"
                ads.append({
                    "id": ad_id,
                    "title": real_title_tag.get_text(strip=True),
                    "price": price_tag.get_text(strip=True) if price_tag else "Consulte",
                    "url": href,
                    "location": "São Sebastião",
                    "category": "Venda"
                })
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro Adimov: {e}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fim ADIMOV: {len(ads)} ads.")
    return ads

async def process_owner_contacts():
    """Busca novos proprietários (owners) e inicia o fluxo de contato via IA."""
    print("Iniciando fluxo de contato via IA...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, price, url, location FROM imoveis WHERE ad_type = 'owner' AND contacted = 0 AND source_site = 'olx' LIMIT 5")
    pending = cursor.fetchall()
    
    if not pending:
        print("Nenhum proprietário pendente de contato.")
        conn.close()
        return

    try:
        ai = AIContactLogic()
        if not hasattr(ai, 'draft_authorization_message'):
            print("AVISO: AIContactLogic não possui o método draft_authorization_message. Verifique se o arquivo ai_contact_logic.py está correto.")
            conn.close()
            return

        for ad in pending:
            ad_id, title, price, url, location = ad
            ad_details = {"title": title, "price": price, "location": location, "url": url, "professionalAd": False}
            print(f"Gerando proposta para: {title}")
            message = await ai.draft_authorization_message(ad_details)
            print(f"--- MENSAGEM IA ---\n{message}\n-------------------")
            cursor.execute("UPDATE imoveis SET contacted = 1 WHERE id = ?", (ad_id,))
            conn.commit()
    except Exception as e:
        print(f"Erro no process_owner_contacts: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()

async def main():
    print(f"--- Início da Rodada de Monitoramento: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    init_db()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        page = await context.new_page()
        
        # 1. OLX - Donos
        try:
            ads_olx_owners = await scrape_olx(page, "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao?f=p", "owner")
            save_new_imoveis(ads_olx_owners, "olx", "owner")
        except Exception as e:
            print(f"Erro crítico OLX Owners: {e}")
        
        # 2. OLX - Profissionais
        try:
            ads_olx_prof = await scrape_olx(page, "https://www.olx.com.br/imoveis/estado-sp/vale-do-paraiba-e-litoral-norte/sao-sebastiao?f=c", "competitor")
            save_new_imoveis(ads_olx_prof, "olx", "competitor")
        except Exception as e:
            print(f"Erro crítico OLX Professional: {e}")
        
        # 3. Riviera Imóveis
        try:
            ads_riviera = await scrape_riviera(page)
            save_new_imoveis(ads_riviera, "riviera", "competitor")
        except Exception as e:
            print(f"Erro crítico Riviera: {e}")
        
        # 4. IZ Imóveis
        try:
            ads_iz = await scrape_iz(page)
            save_new_imoveis(ads_iz, "iz", "competitor")
        except Exception as e:
            print(f"Erro crítico IZ: {e}")
        
        # 5. Tropical Imobiliária (Desativado temporariamente - Bloqueio antibot forte)
        # try:
        #     ads_tropical = await scrape_tropical(page)
        #     save_new_imoveis(ads_tropical, "tropical", "competitor")
        # except Exception as e:
        #     print(f"Erro crítico Tropical: {e}")
        
        # 6. Adimov
        try:
            ads_adimov = await scrape_adimov(page)
            save_new_imoveis(ads_adimov, "adimov", "competitor")
        except Exception as e:
            print(f"Erro crítico Adimov: {e}")
        
        # Notificar novos via Telegram (Bot)
        try:
            notify_new_ads()
        except Exception as e:
            print(f"Erro ao notificar: {e}")
        
        # O fluxo de IA de contato foi desativado conforme solicitado
        
        await browser.close()
    print(f"--- Fim da Rodada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

if __name__ == "__main__":
    asyncio.run(main())

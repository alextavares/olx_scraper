import requests

TOKEN = "8744563469:AAFgKvhcPPSG-QWU19aWJGVZZAvswcd29JM"
url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"

try:
    res = requests.get(url).json()
    if res.get("ok") and len(res["result"]) > 0:
        found_chat_id = None
        for update in res["result"]:
            if "message" in update:
                found_chat_id = update["message"]["chat"]["id"]
                print(f"CHAT_ID={found_chat_id}")
                break
        if not found_chat_id:
            print("NO_MESSAGES")
    else:
        print("NO_MESSAGES")
except Exception as e:
    print(f"ERROR: {e}")

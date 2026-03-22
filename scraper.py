import requests
from bs4 import BeautifulSoup
import json
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FILE_NAME = 'bcv_rates.json'

def get_bcv_rates():
    url = "https://www.bcv.org.ve/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        response = requests.get(url, headers=headers, verify=False, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Error conectando al BCV: {e}")
        return

    def extract_rate(div_id):
        try:
            val = soup.select_one(f"div#{div_id} .centrado strong").text.strip()
            return float(val.replace('.', '').replace(',', '.'))
        except:
            return 0.0

    rates = {
        "usd": extract_rate("dolar"),
        "eur": extract_rate("euro"),
        "cny": extract_rate("yuan"),
        "try": extract_rate("lira"),
        "rub": extract_rate("rublo")
    }

    try:
        date_span = soup.select_one("span.date-display-single")
        fecha_valor = date_span['content'][:10]  # "2026-03-23"
    except:
        print("No se encontró la fecha en el BCV")
        return

    if not fecha_valor or rates["usd"] == 0.0:
        print("Datos inválidos, no se guarda nada")
        return

    # La fecha que guardamos es la ANTERIOR a la fecha valor
    # (el BCV publica hoy la tasa de mañana, nosotros la guardamos con la fecha de hoy)
    from datetime import datetime, timedelta
    fecha_valor_dt = datetime.strptime(fecha_valor, "%Y-%m-%d")
    fecha_hoy = fecha_valor_dt - timedelta(days=1)
    # Si la fecha valor es lunes, la tasa fue publicada el viernes (restar 3 días)
    if fecha_valor_dt.weekday() == 0:  # 0 = lunes
        fecha_hoy = fecha_valor_dt - timedelta(days=3)
    fecha_str = fecha_hoy.strftime("%Y-%m-%d")

    history = {}
    if os.path.exists(FILE_NAME):
        with open(FILE_NAME, 'r', encoding='utf-8') as f:
            try:
                history = json.load(f)
            except:
                pass

    history[fecha_str] = rates

    sorted_dates = sorted(history.keys())[-60:]
    trimmed_history = {k: history[k] for k in sorted_dates}

    with open(FILE_NAME, 'w', encoding='utf-8') as f:
        json.dump(trimmed_history, f, indent=2)
    print(f"Tasa guardada para: {fecha_str} → USD {rates['usd']}")

if __name__ == "__main__":
    get_bcv_rates()

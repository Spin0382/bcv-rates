import requests
from bs4 import BeautifulSoup
import json
import os
import urllib3
import re
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuración por defecto
DEFAULT_CONFIG = {
    'LOG_LEVEL': 'INFO',
    'BCV_TIMEOUT': '25',
    'BCV_MAX_DAILY_CHANGE': '0.4',
    'BCV_MAX_AGE_DAYS': '7',
    'BCV_RETRY_TOTAL': '5',
    'BCV_RETRY_BACKOFF': '1'
}

for key, default in DEFAULT_CONFIG.items():
    if key not in os.environ:
        os.environ[key] = default

# Configuración de Logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FILE_NAME = 'bcv_rates.json'
TIMEOUT = int(os.getenv('BCV_TIMEOUT', '25'))

def parse_venezuelan_number(val):
    """Parsea números en formato venezolano (puntos de miles y coma decimal)"""
    try:
        if not val or not isinstance(val, str):
            return None
        
        val = val.strip()
        if not val:
            return None
        
        if ',' in val:
            val = val.replace('.', '').replace(',', '.')
        elif '.' in val and val.count('.') > 1:
            val = val.replace('.', '')
        
        result = float(val)
        return result if result > 0 else None
        
    except (ValueError, AttributeError) as e:
        logger.debug(f"Error parseando '{val}': {e}")
        return None

def validate_rates(rates, historical_rates=None):
    """Validación lógica de las tasas extraídas"""
    if rates["usd"] is None or rates["usd"] <= 0:
        return False
    
    # Rango de seguridad amplio
    if rates["usd"] > 2000 or rates["usd"] < 1:
        logger.warning(f"USD fuera de rango lógico: {rates['usd']}")
        return False
    
    # Validación contextual
    if historical_rates:
        dates = sorted(historical_rates.keys())
        if dates:
            latest_usd = historical_rates[dates[-1]].get("usd")
            if latest_usd and latest_usd > 0:
                change_pct = abs(rates["usd"] - latest_usd) / latest_usd
                max_change = float(os.getenv('BCV_MAX_DAILY_CHANGE', '0.4'))
                
                if change_pct > max_change:
                    if change_pct > 1.0:  # Cambio >100% bloqueado
                        logger.error(f"Cambio extremo bloqueado: {change_pct:.1%}")
                        return False
                    else:
                        logger.warning(f"Cambio abrupto permitido: {change_pct:.1%}")
    
    return True

def extract_date(soup):
    """Extrae la fecha valor con múltiples estrategias"""
    try:
        # Estrategia 1: Atributo content
        date_span = soup.select_one("span.date-display-single")
        if date_span:
            if date_span.has_attr('content') and date_span['content']:
                return date_span['content'][:10]
            if date_span.text:
                matches = re.findall(r'\d{4}-\d{2}-\d{2}', date_span.text)
                if matches:
                    return matches[0]
        
        # Estrategia 2: Búsqueda global
        matches = re.findall(r'\d{4}-\d{2}-\d{2}', soup.get_text())
        if matches:
            return matches[0]
        
        # Estrategia 3: Fechas en español
        months = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        
        text = soup.get_text().lower()
        for month_name, month_num in months.items():
            pattern = rf'(\d{{1,2}})\s+de\s+{month_name}\s+de\s+(\d{{4}})'
            match = re.search(pattern, text)
            if match:
                day, year = match.groups()
                return f"{year}-{month_num:02d}-{int(day):02d}"
                
    except Exception as e:
        logger.debug(f"Error en extract_date: {e}")
    
    return None

def get_bcv_rates():
    url = "https://www.bcv.org.ve/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    session = requests.Session()
    retry_total = int(os.getenv('BCV_RETRY_TOTAL', '5'))
    retry_backoff = int(os.getenv('BCV_RETRY_BACKOFF', '1'))
    
    retries = Retry(
        total=retry_total,
        backoff_factor=retry_backoff,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.get(url, headers=headers, verify=False, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        return False

    def extract_rate_flexible(div_id):
        selectors = [
            f"div#{div_id} .centrado strong",
            f"div#{div_id} strong",
            f"#{div_id} .centrado strong"
        ]
        for selector in selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    val = parse_venezuelan_number(elem.text)
                    if val:
                        return val
            except:
                continue
        return None

    rates = {
        "usd": extract_rate_flexible("dolar"),
        "eur": extract_rate_flexible("euro"),
        "cny": extract_rate_flexible("yuan"),
        "try": extract_rate_flexible("lira"),
        "rub": extract_rate_flexible("rublo")
    }
    
    fecha_str = extract_date(soup)
    
    history = {}
    if os.path.exists(FILE_NAME):
        try:
            with open(FILE_NAME, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f"Historial corrupto: {e}")
        except Exception as e:
            logger.warning(f"No se pudo leer historial: {e}")

    if not fecha_str:
        logger.error("No se pudo extraer la fecha")
        return False
    
    if not validate_rates(rates, history):
        logger.error(f"Validación fallida para {fecha_str}")
        return False

    history[fecha_str] = rates
    sorted_keys = sorted(history.keys())[-60:]
    trimmed_history = {k: history[k] for k in sorted_keys}

    temp_file = f"{FILE_NAME}.tmp"
    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(trimmed_history, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, FILE_NAME)
        logger.info(f"✅ Guardado: {fecha_str} | USD: {rates['usd']:.2f}")
        return True
    except Exception as e:
        logger.error(f"Error al escribir archivo: {e}")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        return False

def health_check():
    """Verifica la integridad y actualidad del JSON"""
    if not os.path.exists(FILE_NAME):
        logger.warning("Archivo JSON no existe")
        return False
    
    try:
        with open(FILE_NAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data:
            logger.warning("Archivo JSON vacío")
            return False
        
        last_date = max(data.keys())
        
        try:
            last_date_obj = datetime.fromisoformat(last_date)
        except ValueError:
            logger.error(f"Formato de fecha inválido: {last_date}")
            return False
        
        today = datetime.now()
        days_diff = 0 if last_date_obj > today else (today - last_date_obj).days
        
        max_age = int(os.getenv('BCV_MAX_AGE_DAYS', '7'))
        
        if days_diff > max_age:
            logger.warning(f"Datos desactualizados: {days_diff} días (máx: {max_age})")
            return False
        
        last_usd = data[last_date].get("usd", 0)
        if last_usd <= 0:
            logger.warning(f"Última tasa USD inválida: {last_usd}")
            return False
        
        logger.info(f"Health check OK: {last_date} (antigüedad: {days_diff} días, USD: {last_usd:.2f})")
        return True
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON corrupto: {e}")
        return False
    except Exception as e:
        logger.error(f"Health check falló: {e}")
        return False

if __name__ == "__main__":
    try:
        success = get_bcv_rates()
        
        if success:
            if health_check():
                logger.info("🚀 Proceso completado exitosamente")
                exit(0)
            else:
                logger.warning("⚠️ Datos guardados pero health check falló")
                exit(1)
        else:
            logger.error("❌ Scraping fallido - no se guardaron datos")
            exit(2)
    except KeyboardInterrupt:
        logger.warning("Proceso interrumpido por usuario")
        exit(130)
    except Exception as e:
        logger.exception(f"Error inesperado: {e}")
        exit(3)

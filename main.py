"""
waVES API v2.0 - Smart Financial Ledger
Autor: Gravity Labs (Backend Team)
Descripción: Backend con inteligencia de vigencia bancaria (BCV/Binance).
Soporta consultas históricas con 'fallback' para fines de semana.
"""

from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
import urllib3
import sqlite3
from datetime import datetime, timedelta
import os

# Desactivar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="waVES API", version="2.0.0")

# --- CONFIGURACIÓN DE BASE DE DATOS ---
DB_NAME = "tasas_smart.db"

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Crea la tabla con soporte para vigencia inteligente."""
    try:
        conn = get_db()
        c = conn.cursor()
        # Tabla optimizada: 'effective_date' es la clave de la vigencia
        c.execute('''
            CREATE TABLE IF NOT EXISTS historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,            -- BCV, BINANCE
                currency TEXT,          -- USD, EUR, USDT
                rate REAL,
                effective_date TEXT,    -- YYYY-MM-DD (Fecha de validez legal)
                fetched_at TEXT         -- YYYY-MM-DD HH:MM:SS (Fecha de captura)
            )
        ''')
        # Índice para acelerar las búsquedas de RINDE
        c.execute('CREATE INDEX IF NOT EXISTS idx_date ON historial(effective_date)')
        conn.commit()
        conn.close()
        limpiar_datos_antiguos()
    except Exception as e:
        print(f"[CRITICAL] Error DB Init: {e}")

def limpiar_datos_antiguos():
    """Mantiene solo los últimos 100 días de historia."""
    try:
        conn = get_db()
        c = conn.cursor()
        # Borrar registros con vigencia mayor a 100 días
        fecha_limite = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
        c.execute("DELETE FROM historial WHERE effective_date < ?", (fecha_limite,))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[MAINTENANCE] Error limpiando DB: {e}")

# Inicializar al arrancar
init_db()

# --- LÓGICA DE NEGOCIO: VIGENCIA BANCARIA ---

def calcular_vigencia(source: str) -> str:
    """
    Determina para qué día es válida la tasa que estamos descargando HOY.
    Reglas:
    1. Binance: Siempre es HOY.
    2. BCV (Viernes > 3:30 PM): Es válida para el Lunes.
    3. BCV (Semana > 4:00 PM): Es válida para Mañana.
    4. BCV (Fines de Semana): Es válida para el Lunes.
    """
    ahora = datetime.now()
    hoy_str = ahora.strftime("%Y-%m-%d")
    
    if source == "BINANCE":
        return hoy_str

    if source == "BCV":
        # Viernes (4) después de las 15:30 -> Lunes
        if ahora.weekday() == 4 and (ahora.hour > 15 or (ahora.hour == 15 and ahora.minute >= 30)):
            return (ahora + timedelta(days=3)).strftime("%Y-%m-%d")
        
        # Sábado (5) o Domingo (6) -> Lunes
        elif ahora.weekday() in [5, 6]:
            dias_para_lunes = 7 - ahora.weekday()
            return (ahora + timedelta(days=dias_para_lunes)).strftime("%Y-%m-%d")
        
        # Lunes a Jueves después de las 16:00 -> Mañana
        elif ahora.hour >= 16:
            return (ahora + timedelta(days=1)).strftime("%Y-%m-%d")
            
    return hoy_str

def guardar_tasa(source, currency, rate):
    """Guarda la tasa aplicando la lógica de vigencia."""
    effective_date = calcular_vigencia(source)
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        conn = get_db()
        c = conn.cursor()
        
        # Evitar duplicados: Si ya tengo una tasa para esa Fecha y Fuente, actualizo el valor (por si corrigieron)
        c.execute("SELECT id FROM historial WHERE source=? AND currency=? AND effective_date=?", 
                  (source, currency, effective_date))
        existe = c.fetchone()
        
        if existe:
            # Actualizamos el valor por si cambió (ej. volatilidad Binance)
            c.execute("UPDATE historial SET rate=?, fetched_at=? WHERE id=?", 
                      (rate, fetched_at, existe['id']))
        else:
            c.execute("INSERT INTO historial (source, currency, rate, effective_date, fetched_at) VALUES (?, ?, ?, ?, ?)",
                      (source, currency, rate, effective_date, fetched_at))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Guardando DB: {e}")

# --- SCRAPERS (Optimizados) ---

def actualizar_tasas():
    """Ejecuta scraping y guarda con fecha inteligente."""
    # 1. BCV
    try:
        resp = requests.get("https://www.bcv.org.ve/", verify=False, timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.content, 'html.parser')
            usd = float(soup.find('div', {'id': 'dolar'}).find('strong').text.strip().replace(',', '.'))
            eur = float(soup.find('div', {'id': 'euro'}).find('strong').text.strip().replace(',', '.'))
            guardar_tasa("BCV", "USD", usd)
            guardar_tasa("BCV", "EUR", eur)
    except Exception as e:
        print(f"[SCRAPER] BCV Error: {e}")

    # 2. BINANCE
    try:
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        payload = {"asset": "USDT", "fiat": "VES", "merchantCheck": False, "page": 1, "rows": 5, "tradeType": "BUY", "transAmount": "500"}
        headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
        resp = requests.post(url, json=payload, headers=headers, timeout=5)
        data = resp.json()
        if data["code"] == "000000":
            precios = [float(ad['adv']['price']) for ad in data["data"]]
            promedio = round(sum(precios) / len(precios), 2)
            guardar_tasa("BINANCE", "USDT", promedio)
    except Exception as e:
        print(f"[SCRAPER] Binance Error: {e}")

# --- ENDPOINTS ---

@app.get("/")
def home():
    """Endpoint ligero para el Cron Job."""
    return {"status": "alive", "version": "2.0.0"}

@app.get("/api/tasas")
def trigger_update():
    """Endpoint manual para forzar actualización (usado por Admin o Cron secundario)."""
    actualizar_tasas()
    return {"status": "updated", "timestamp": datetime.now()}

@app.get("/api/consultar")
def consultar_smart(
    fecha: str = Query(..., description="Fecha YYYY-MM-DD"), 
    fuente: str = "BCV", 
    moneda: str = "USD"
):
    """
    EL CEREBRO DE RINDE:
    Devuelve la tasa vigente para la fecha solicitada.
    Si es Domingo y no hay tasa, devuelve el Viernes (Fallback).
    """
    conn = get_db()
    c = conn.cursor()
    
    # Lógica de Fallback: Buscar la tasa más reciente que sea <= a la fecha solicitada
    c.execute("""
        SELECT rate, effective_date, fetched_at 
        FROM historial 
        WHERE source=? AND currency=? AND effective_date <= ? 
        ORDER BY effective_date DESC 
        LIMIT 1
    """, (fuente.upper(), moneda.upper(), fecha))
    
    dato = c.fetchone()
    conn.close()
    
    if dato:
        es_exacto = (dato['effective_date'] == fecha)
        return {
            "query": {"fecha": fecha, "fuente": fuente, "moneda": moneda},
            "data": {
                "tasa": dato['rate'],
                "vigencia": dato['effective_date'],
                "capturado": dato['fetched_at'],
                "metodo": "exacto" if es_exacto else "fallback_historico" # RINDE sabrá si usó datos pasados
            }
        }
    else:
        # Si no hay NADA (ej. base de datos vacía), intentamos scrapear ahora mismo
        actualizar_tasas()
        raise HTTPException(status_code=404, detail="Datos no disponibles aún. Intente en 5 segundos.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
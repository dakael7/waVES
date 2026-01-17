"""
waVES API v1.0
Autor: DaKael7
Descripción: Backend para obtener, cachear y persistir tasas de cambio (BCV/Binance).
Incluye mantenimiento automático de base de datos (elimina registros > 6 meses).
"""

from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
import time
import urllib3
import sqlite3
from datetime import datetime

# Desactivar advertencias de SSL (necesario para la web del BCV)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="HarVESter API", version="1.0.0")

# --- CONFIGURACIÓN DE BASE DE DATOS (SQLite) ---

DB_NAME = "tasas.db"

def limpiar_datos_antiguos():
    """
    MANTENIMIENTO: Elimina registros con más de 6 meses de antigüedad.
    Se ejecuta al iniciar y en cada actualización para mantener la DB ligera.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        # SQL para borrar todo lo que sea anterior a 'hace 6 meses'
        c.execute("DELETE FROM historial WHERE fecha < datetime('now', '-6 months')")
        eliminados = c.rowcount
        conn.commit()
        conn.close()
        if eliminados > 0:
            print(f"[MANTENIMIENTO] Se eliminaron {eliminados} registros antiguos.")
    except Exception as e:
        print(f"[ERROR] Fallo limpieza de DB: {e}")

def init_db():
    """Inicializa la base de datos, crea la tabla y ejecuta limpieza."""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                fuente TEXT,
                moneda TEXT,
                valor REAL
            )
        ''')
        conn.commit()
        conn.close()
        print("[INFO] Base de datos inicializada correctamente.")
        
        # Ejecutar limpieza preventiva al arrancar
        limpiar_datos_antiguos()
        
    except Exception as e:
        print(f"[ERROR] Fallo al iniciar DB: {e}")

# Inicializamos la DB al arrancar el script
init_db()

def guardar_historial(fuente, moneda, valor):
    """Guarda un registro de tasa en la base de datos."""
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO historial (fecha, fuente, moneda, valor) VALUES (?, ?, ?, ?)",
                  (fecha_actual, fuente, moneda, valor))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] No se pudo guardar en historial: {e}")

# --- MEMORIA CACHE (RAM) ---
datos_en_memoria = {
    "tasas": {
        "bcv_usd": 0.0,
        "bcv_eur": 0.0,
        "binance_usdt": 0.0
    },
    "ultima_actualizacion": 0
}

# --- MOTORES DE EXTRACCIÓN (SCRAPERS) ---

def obtener_bcv():
    """Scraping directo a bcv.org.ve"""
    url = "https://www.bcv.org.ve/"
    try:
        # Timeout de 10s y verify=False para evitar errores de cert
        response = requests.get(url, verify=False, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Selectores del HTML actual del BCV
            usd_tag = soup.find('div', {'id': 'dolar'})
            eur_tag = soup.find('div', {'id': 'euro'})
            
            if usd_tag and eur_tag:
                usd = float(usd_tag.find('strong').text.strip().replace(',', '.'))
                eur = float(eur_tag.find('strong').text.strip().replace(',', '.'))
                
                # Guardamos en Historial
                guardar_historial("BCV", "USD", usd)
                guardar_historial("BCV", "EUR", eur)
                
                return usd, eur
    except Exception as e:
        print(f"[ERROR] Fallo scraping BCV: {e}")
    
    return None, None

def obtener_binance():
    """Consumo de API P2P de Binance"""
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/json"
    }
    
    payload = {
        "asset": "USDT",
        "fiat": "VES",
        "merchantCheck": False,
        "page": 1,
        "rows": 5, 
        "tradeType": "BUY",
        "transAmount": "500" 
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        data = response.json()
        
        if data["code"] == "000000" and data["data"]:
            anuncios = data["data"]
            precios = [float(ad['adv']['price']) for ad in anuncios]
            promedio = sum(precios) / len(precios)
            promedio_redondeado = round(promedio, 2)
            
            # Guardamos en Historial
            guardar_historial("BINANCE", "USDT", promedio_redondeado)
            
            return promedio_redondeado
            
    except Exception as e:
        print(f"[ERROR] Fallo API Binance: {e}")
        
    return None

# --- ENDPOINTS ---

@app.get("/")
def home():
    return {"estado": "en linea", "servicio": "HarVESter API"}

@app.get("/api/tasas")
def get_tasas():
    """Devuelve tasas actuales. Actualiza si el cache expiró."""
    tiempo_actual = time.time()
    ultimo_tiempo = datos_en_memoria["ultima_actualizacion"]
    
    # Si pasaron más de 10 min (600s), actualizamos
    if tiempo_actual - ultimo_tiempo > 600:
        print("[INFO] Cache expirado. Buscando nuevas tasas...")
        
        # 1. Limpieza de datos viejos (> 6 meses)
        limpiar_datos_antiguos()
        
        # 2. Obtención de datos nuevos
        nuevo_usd, nuevo_eur = obtener_bcv()
        nuevo_usdt = obtener_binance()
        
        # Solo actualizamos memoria si obtuvimos éxito
        if nuevo_usd and nuevo_eur:
            datos_en_memoria["tasas"]["bcv_usd"] = nuevo_usd
            datos_en_memoria["tasas"]["bcv_eur"] = nuevo_eur
            
        if nuevo_usdt:
            datos_en_memoria["tasas"]["binance_usdt"] = nuevo_usdt
            
        datos_en_memoria["ultima_actualizacion"] = tiempo_actual
    
    return datos_en_memoria["tasas"]

@app.get("/api/historial")
def get_historial(limite: int = 50):
    """Devuelve los últimos 'limite' registros de la base de datos."""
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute("SELECT * FROM historial ORDER BY id DESC LIMIT ?", (limite,))
        filas = c.fetchall()
        conn.close()
        
        return [dict(fila) for fila in filas]
        
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
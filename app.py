from flask import Flask, jsonify
from flask_cors import CORS
from scraper import TAMSScraperFinal
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time
import pytz

# =====================================================
# CONFIGURACIÓN
# =====================================================
app = Flask(__name__)
CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Zona horaria de Argentina
ARGENTINA_TZ = pytz.timezone('America/Argentina/Buenos_Aires')

# =====================================================
# NORMALIZACIÓN DE DATOS
# =====================================================
def limpiar_campo(obj: Dict, claves_posibles: list) -> str:
    """Busca un campo en múltiples variantes de encoding"""
    for clave in claves_posibles:
        if clave in obj and obj[clave]:
            return str(obj[clave]).strip()
    return "---"

def extraer_fecha_hora(raw: str) -> tuple:
    """Extrae fecha (DD/MM) y hora (HH:MM) de strings como '08/12 19:30'"""
    if not raw:
        return ("", "")
    
    # Limpiar el string
    raw = raw.strip()
    
    # Si tiene espacio, separar fecha y hora
    if " " in raw:
        partes = raw.split()
        if len(partes) >= 2:
            fecha = partes[0].replace("|", "/")  # 08|12 -> 08/12
            hora = partes[1]
            return (fecha, hora)
        elif len(partes) == 1:
            # Solo hora
            return ("", partes[0])
    
    # Si no tiene espacio pero tiene /, probablemente solo sea hora
    if "/" in raw or "|" in raw:
        return (raw.replace("|", "/"), "")
    
    # Solo hora
    return ("", raw)

def limpiar_hora(raw: str) -> str:
    """Extrae solo HH:MM - mantiene compatibilidad"""
    _, hora = extraer_fecha_hora(raw)
    return hora

def normalizar_vuelo(vuelo: Dict, tipo: str) -> Dict:
    """
    Transforma el formato crudo de TAMS al formato limpio esperado por el frontend.
    Maneja todos los encodings posibles de caracteres especiales.
    """
    # Extraer CÍA con múltiples variantes de encoding
    cia = limpiar_campo(vuelo, [
        "Cia.",       # Normal
        "CÃa.",       # Latin-1 mal interpretado
        "C\u00c3\u00ada.",  # UTF-8 doble encoding
        "Cía.",       # Con tilde correcta
        "Cia"         # Sin punto
    ])
    
    # Número de vuelo
    num = vuelo.get("Vuelo", "")
    vuelo_full = f"{cia} {num}".strip()
    
    # Matrícula con variantes
    matricula = limpiar_campo(vuelo, [
        "Matricula",
        "MatrÃcula",
        "Matr\u00c3\u00adcula",
        "Matrícula"
    ])
    
    # Posición con variantes
    posicion = limpiar_campo(vuelo, [
        "Posicion",
        "PosiciÃ³n",
        "Posici\u00c3\u00b3n",
        "Posición"
    ])
    
    # Campos específicos por tipo
    if tipo == "dep":
        lugar = vuelo.get("Destino", "---")
        dato_extra = vuelo.get("Puerta", "---")
        
        # Extraer fecha y hora de STD, ETD, ATD
        fecha_prog, prog = extraer_fecha_hora(vuelo.get("STD", ""))
        fecha_est, est = extraer_fecha_hora(vuelo.get("ETD", ""))
        fecha_real, real = extraer_fecha_hora(vuelo.get("ATD", ""))
        
        # Usar la primera fecha disponible
        fecha = fecha_prog or fecha_est or fecha_real or ""
        
    else:  # arribos
        lugar = vuelo.get("Origen", "---")
        dato_extra = vuelo.get("Cinta", "---")
        
        # Extraer fecha y hora de STA, ETA, ATA
        fecha_prog, prog = extraer_fecha_hora(vuelo.get("STA", ""))
        fecha_est, est = extraer_fecha_hora(vuelo.get("ETA", ""))
        fecha_real, real = extraer_fecha_hora(vuelo.get("ATA", ""))
        
        # Usar la primera fecha disponible
        fecha = fecha_prog or fecha_est or fecha_real or ""
    
    estado = vuelo.get("Remark", "")
    
    return {
        "vuelo": vuelo_full,
        "lugar": lugar,
        "fecha": fecha,  # ✅ NUEVO CAMPO
        "hora_prog": prog,
        "hora_est": est,
        "hora_real": real,
        "matricula": matricula,
        "posicion": posicion,
        "dato_extra": dato_extra,
        "estado": estado
    }

# =====================================================
# SISTEMA DE CACHÉ
# =====================================================
class FlightDataCache:
    def __init__(self, ttl_seconds: int = 120):
        self.data: Optional[Dict[str, Any]] = None
        self.timestamp: Optional[datetime] = None
        self.ttl = ttl_seconds
        self.lock = threading.Lock()
        self.scraping_in_progress = False
        self.last_error: Optional[str] = None
        self.scrape_count = 0
        self.hit_count = 0
        self.miss_count = 0
    
    def is_expired(self) -> bool:
        if self.timestamp is None:
            return True
        age = (datetime.now(ARGENTINA_TZ) - self.timestamp).total_seconds()
        return age >= self.ttl
    
    def get_age(self) -> Optional[float]:
        if self.timestamp is None:
            return None
        return (datetime.now(ARGENTINA_TZ) - self.timestamp).total_seconds()
    
    def get_or_refresh(self) -> Dict[str, Any]:
        # Fast path: caché válido
        if self.data is not None and not self.is_expired():
            with self.lock:
                self.hit_count += 1
            logger.info(f"✅ CACHÉ HIT - Edad: {self.get_age():.1f}s")
            return self.data
        
        # Slow path: necesita scraping
        with self.lock:
            # Double-check
            if self.data is not None and not self.is_expired():
                self.hit_count += 1
                return self.data
            
            if self.scraping_in_progress:
                logger.info("⏳ Scraping en progreso...")
                self.lock.release()
                time.sleep(2)
                self.lock.acquire()
                if self.data is not None and not self.is_expired():
                    self.hit_count += 1
                    return self.data
            
            self.scraping_in_progress = True
            self.miss_count += 1
        
        # Scrapear
        try:
            logger.info("🔄 CACHÉ MISS - Scrapeando...")
            start_time = time.time()
            scraper = TAMSScraperFinal()
            arribos_raw, partidas_raw = scraper.scrape_all_flights()
            
            # ⚡ NORMALIZAR DATOS AQUÍ
            arribos_limpios = [normalizar_vuelo(v, "arr") for v in arribos_raw]
            partidas_limpias = [normalizar_vuelo(v, "dep") for v in partidas_raw]
            
            elapsed = time.time() - start_time
            
            new_data = {
                'arribos': arribos_limpios,
                'partidas': partidas_limpias,
                'timestamp': datetime.now(ARGENTINA_TZ).isoformat(),
                'scrape_time': round(elapsed, 2),
                'total_flights': len(arribos_limpios) + len(partidas_limpias)
            }
            
            with self.lock:
                self.data = new_data
                self.timestamp = datetime.now(ARGENTINA_TZ)
                self.last_error = None
                self.scrape_count += 1
                self.scraping_in_progress = False
            
            logger.info(f"✅ Scraping OK - {new_data['total_flights']} vuelos en {elapsed:.2f}s")
            logger.info(f"   Partidas: {len(partidas_limpias)} | Arribos: {len(arribos_limpios)}")
            
            # DEBUG: Ver primer vuelo
            if partidas_limpias:
                logger.info(f"   Primera partida: {partidas_limpias[0]['vuelo']} - Fecha: {partidas_limpias[0].get('fecha', 'N/A')}")
            
            return new_data
            
        except Exception as e:
            logger.error(f"❌ Error scraping: {e}")
            with self.lock:
                self.last_error = str(e)
                self.scraping_in_progress = False
            
            if self.data is not None:
                logger.warning("⚠️ Usando caché expirado")
                stale_data = self.data.copy()
                stale_data['warning'] = 'Datos desactualizados'
                return stale_data
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hit_count + self.miss_count
            hit_rate = (self.hit_count / total * 100) if total > 0 else 0
            return {
                'hits': self.hit_count,
                'misses': self.miss_count,
                'hit_rate': round(hit_rate, 1),
                'scrape_count': self.scrape_count,
                'cache_age': self.get_age(),
                'ttl': self.ttl,
                'is_expired': self.is_expired(),
                'has_data': self.data is not None,
                'last_error': self.last_error
            }
    
    def clear(self):
        with self.lock:
            self.data = None
            self.timestamp = None

flight_cache = FlightDataCache(ttl_seconds=120)

# =====================================================
# ENDPOINTS
# =====================================================
@app.route('/datos-limpios', methods=['GET'])
def datos_limpios():
    try:
        data = flight_cache.get_or_refresh()
        return jsonify(data), 200
    except Exception as e:
        logger.exception("Error obteniendo datos")
        return jsonify({
            'error': str(e),
            'partidas': [],
            'arribos': []
        }), 500

@app.route('/health', methods=['GET'])
def health():
    stats = flight_cache.get_stats()
    status = 'healthy' if stats['has_data'] and not stats['is_expired'] else 'degraded'
    return jsonify({
        'status': status,
        'timestamp': datetime.now(ARGENTINA_TZ).isoformat(),
        'cache': stats
    }), 200

@app.route('/stats', methods=['GET'])
def stats():
    return jsonify(flight_cache.get_stats()), 200

@app.route('/cache/clear', methods=['POST'])
def clear_cache():
    flight_cache.clear()
    return jsonify({'message': 'Caché limpiado'}), 200

@app.route('/cache/refresh', methods=['POST'])
def refresh_cache():
    try:
        with flight_cache.lock:
            flight_cache.timestamp = datetime.now(ARGENTINA_TZ) - timedelta(seconds=flight_cache.ttl + 1)
        data = flight_cache.get_or_refresh()
        return jsonify({'message': 'Caché actualizado', 'flights': data['total_flights']}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'AEP Flight Data API',
        'version': '2.2 - Con fecha y zona horaria',
        'endpoints': {
            'GET /datos-limpios': 'Datos normalizados con fecha',
            'GET /health': 'Estado del sistema',
            'GET /stats': 'Estadísticas',
            'POST /cache/refresh': 'Forzar actualización'
        }
    }), 200

if __name__ == "__main__":
    logger.info("🚀 Iniciando servidor con normalización y fecha...")
    app.run(debug=False, host="0.0.0.0", port=5000)

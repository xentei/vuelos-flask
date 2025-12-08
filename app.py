from flask import Flask, jsonify, request
from flask_cors import CORS
from scraper import TAMSScraperFinal
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time

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

# =====================================================
# SISTEMA DE CACHÉ THREAD-SAFE
# =====================================================
class FlightDataCache:
    """Caché inteligente con threading y TTL"""
    
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
        """Verifica si el caché expiró"""
        if self.timestamp is None:
            return True
        age = (datetime.now() - self.timestamp).total_seconds()
        return age >= self.ttl
    
    def get_age(self) -> Optional[float]:
        """Retorna edad del caché en segundos"""
        if self.timestamp is None:
            return None
        return (datetime.now() - self.timestamp).total_seconds()
    
    def get_or_refresh(self) -> Dict[str, Any]:
        """Obtiene datos del caché o scrapea si es necesario"""
        
        # Fast path: caché válido
        if self.data is not None and not self.is_expired():
            with self.lock:
                self.hit_count += 1
            logger.info(f"✅ CACHÉ HIT - Edad: {self.get_age():.1f}s")
            return self.data
        
        # Slow path: necesita scraping
        with self.lock:
            # Double-check: otro thread pudo haber actualizado mientras esperábamos
            if self.data is not None and not self.is_expired():
                self.hit_count += 1
                logger.info(f"✅ CACHÉ HIT (double-check) - Edad: {self.get_age():.1f}s")
                return self.data
            
            # Si otro thread está scrapeando, esperar un poco
            if self.scraping_in_progress:
                logger.info("⏳ Scraping en progreso, esperando...")
                # Release lock temporalmente
                self.lock.release()
                time.sleep(2)
                self.lock.acquire()
                
                # Verificar si el otro thread terminó
                if self.data is not None and not self.is_expired():
                    self.hit_count += 1
                    logger.info("✅ CACHÉ actualizado por otro thread")
                    return self.data
            
            # Marcar que estamos scrapeando
            self.scraping_in_progress = True
            self.miss_count += 1
        
        # Scrapear (fuera del lock para no bloquear otros threads)
        try:
            logger.info("🔄 CACHÉ MISS - Iniciando scraping...")
            start_time = time.time()
            
            scraper = TAMSScraperFinal()
            arribos, partidas = scraper.scrape_all_flights()
            
            elapsed = time.time() - start_time
            
            new_data = {
                'arribos': arribos,
                'partidas': partidas,
                'timestamp': datetime.now().isoformat(),
                'scrape_time': round(elapsed, 2),
                'total_flights': len(arribos) + len(partidas)
            }
            
            # Actualizar caché
            with self.lock:
                self.data = new_data
                self.timestamp = datetime.now()
                self.last_error = None
                self.scrape_count += 1
                self.scraping_in_progress = False
            
            logger.info(f"✅ Scraping exitoso - {new_data['total_flights']} vuelos en {elapsed:.2f}s")
            return new_data
            
        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            
            with self.lock:
                self.last_error = str(e)
                self.scraping_in_progress = False
                
                # Si hay caché viejo, retornarlo como fallback
                if self.data is not None:
                    logger.warning("⚠️ Retornando caché expirado como fallback")
                    stale_data = self.data.copy()
                    stale_data['warning'] = 'Datos desactualizados'
                    stale_data['age_seconds'] = self.get_age()
                    return stale_data
            
            # No hay caché, propagar error
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Estadísticas del caché"""
        with self.lock:
            total_requests = self.hit_count + self.miss_count
            hit_rate = (self.hit_count / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'hits': self.hit_count,
                'misses': self.miss_count,
                'hit_rate': round(hit_rate, 1),
                'scrape_count': self.scrape_count,
                'cache_age': self.get_age(),
                'ttl': self.ttl,
                'is_expired': self.is_expired(),
                'has_data': self.data is not None,
                'last_error': self.last_error,
                'scraping_in_progress': self.scraping_in_progress
            }
    
    def clear(self):
        """Limpia el caché"""
        with self.lock:
            self.data = None
            self.timestamp = None
            logger.info("🗑️ Caché limpiado manualmente")


# Instancia global del caché (TTL: 2 minutos)
flight_cache = FlightDataCache(ttl_seconds=120)


# =====================================================
# ENDPOINTS
# =====================================================

@app.route('/datos-limpios', methods=['GET'])
def datos_limpios():
    """Endpoint principal - Retorna datos de vuelos (con caché)"""
    try:
        data = flight_cache.get_or_refresh()
        return jsonify(data), 200
    except Exception as e:
        logger.exception("Error obteniendo datos")
        return jsonify({
            'error': str(e),
            'message': 'Error al obtener datos de vuelos',
            'timestamp': datetime.now().isoformat()
        }), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    stats = flight_cache.get_stats()
    
    status = 'healthy' if stats['has_data'] and not stats['is_expired'] else 'degraded'
    
    return jsonify({
        'status': status,
        'timestamp': datetime.now().isoformat(),
        'cache': stats
    }), 200


@app.route('/stats', methods=['GET'])
def stats():
    """Estadísticas del caché"""
    return jsonify(flight_cache.get_stats()), 200


@app.route('/cache/clear', methods=['POST'])
def clear_cache():
    """Limpia el caché manualmente (útil para debugging)"""
    flight_cache.clear()
    return jsonify({
        'message': 'Caché limpiado',
        'timestamp': datetime.now().isoformat()
    }), 200


@app.route('/cache/refresh', methods=['POST'])
def refresh_cache():
    """Fuerza un refresh del caché"""
    try:
        # Marcar como expirado para forzar refresh
        with flight_cache.lock:
            flight_cache.timestamp = datetime.now() - timedelta(seconds=flight_cache.ttl + 1)
        
        data = flight_cache.get_or_refresh()
        
        return jsonify({
            'message': 'Caché actualizado',
            'data': data
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/', methods=['GET'])
def index():
    """Página de bienvenida con info de endpoints"""
    return jsonify({
        'service': 'AEP Flight Data API',
        'version': '2.0',
        'endpoints': {
            'GET /datos-limpios': 'Obtener datos de vuelos (con caché)',
            'GET /health': 'Health check y estado del caché',
            'GET /stats': 'Estadísticas del caché',
            'POST /cache/clear': 'Limpiar caché manualmente',
            'POST /cache/refresh': 'Forzar actualización del caché'
        },
        'cache_ttl': f"{flight_cache.ttl} segundos"
    }), 200


# =====================================================
# ERROR HANDLERS
# =====================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint no encontrado'}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.exception("Error 500")
    return jsonify({'error': 'Error interno del servidor'}), 500


# =====================================================
# STARTUP
# =====================================================

@app.before_request
def log_request():
    """Log de cada request"""
    logger.info(f"→ {request.method} {request.path} from {request.remote_addr}")


if __name__ == "__main__":
    logger.info("🚀 Iniciando servidor Flask...")
    logger.info(f"📦 Caché TTL: {flight_cache.ttl} segundos")
    app.run(debug=False, host="0.0.0.0", port=5000)

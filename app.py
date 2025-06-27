from flask import Flask, jsonify
import logging
from scraper import TAMSScraperFinal  # asegúrate de que el archivo se llame scraper.py

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

@app.route('/')
def home():
    return 'API de vuelos funcionando. Usá /vuelos para obtener datos.'

@app.route('/vuelos')
def vuelos():
    try:
        scraper = TAMSScraperFinal()
        arribos, partidas = scraper.scrape_all_flights()
        return jsonify({
            'arribos': arribos,
            'partidas': partidas
        })
    except Exception as e:
        logging.exception("Error en el scraping")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logging.info("Servidor Flask iniciando en http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000)

from flask import Flask, jsonify
import datetime
import logging
from tams_scraper import TAMSScraperFinal  # ✅ debe importar la clase, no otro servidor Flask

app = Flask(__name__)

@app.route('/')
def home():
    return '✅ API de vuelos funcionando'

@app.route('/vuelos')
def vuelos():
    try:
        scraper = TAMSScraperFinal()
        arribos, partidas = scraper.scrape_all_flights()

        data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "arribos": arribos,
            "partidas": partidas
        }

        return jsonify(data)

    except Exception as e:
        logging.exception("Error en el scraping")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    import os
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

from flask import Flask, jsonify
import logging

from scraper import TAMSScraperFinal  # nuevo nombre del archivo del scraper

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
    app.run(host='0.0.0.0', port=5000)

from flask import Flask, jsonify
from scraper import TAMSScraperFinal
import logging
import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route('/')
def obtener_vuelos():
    try:
        scraper = TAMSScraperFinal()
        arrivals, departures = scraper.scrape_all_flights()

        data = {
            'arribos': arrivals,
            'partidas': departures,
            'timestamp': datetime.datetime.now().isoformat()
        }

        return jsonify(data), 200

    except Exception as e:
        logging.exception("Error al obtener vuelos")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000)


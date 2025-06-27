from flask import Flask, jsonify
import datetime

app = Flask(__name__)

@app.route('/')
def home():
    return '✅ API de vuelos funcionando'

@app.route('/vuelos')
def vuelos():
    # Ejemplo fijo; en tu versión real reemplazás con scraping o lectura dinámica
    data = {
        "timestamp": datetime.datetime.now().isoformat(),
        "arribos": [
            {
                "Vuelo": "7040",
                "Tipo": "Arribo",
                "Origen": "GIG",
                "Destino": "AEP",
                "Posicion": "05",
                "STA": "2025-06-27T01:40:00",
                "Cia": "G3",
                "Matricula": "PSGRD"
            }
        ]
    }
    return jsonify(data)

if __name__ == '__main__':
    import os
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

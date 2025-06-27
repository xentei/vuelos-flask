import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import logging
import json
from typing import List, Dict
from fake_useragent import UserAgent

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TAMSScraperFinal:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "http://www.tams.com.ar/organismos/vuelos.aspx"
        self.setup_session()

    def setup_session(self):
        try:
            user_agent = UserAgent().random
        except:
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(headers)

    def get_hidden_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        fields = {}
        for name in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
            input_tag = soup.find('input', {'name': name})
            if input_tag:
                fields[name] = input_tag.get('value', '')
        return fields

    def get_initial_page(self) -> BeautifulSoup:
        res = self.session.get(self.base_url)
        res.raise_for_status()
        return BeautifulSoup(res.text, 'html.parser')

    def change_flight_type_and_search(self, hidden_fields: Dict[str, str], type_code: str) -> BeautifulSoup:
        combo_data = {
            '__EVENTTARGET': 'ddlMovTp',
            '__EVENTARGUMENT': '',
            'ddlMovTp': type_code,
            **hidden_fields
        }
        res_combo = self.session.post(self.base_url, data=combo_data)
        soup_combo = BeautifulSoup(res_combo.text, 'html.parser')
        new_fields = self.get_hidden_fields(soup_combo)
        search_data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            'ddlMovTp': type_code,
            'btnBuscar': 'Buscar',
            **new_fields
        }
        res_search = self.session.post(self.base_url, data=search_data)
        return BeautifulSoup(res_search.text, 'html.parser')

    def parse_flights_table(self, soup: BeautifulSoup, flight_type: str) -> List[Dict]:
        flights = []
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        table = soup.find('table', {'id': table_id})
        if not table:
            return flights
        rows = table.find_all('tr')
        headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) >= len(headers):
                flight = {'Tipo': flight_type}
                for idx, cell in enumerate(cells):
                    flight[headers[idx]] = cell.get_text(strip=True)
                flights.append(flight)
        return flights

    def scrape_all_flights(self) -> tuple:
        soup = self.get_initial_page()
        hidden = self.get_hidden_fields(soup)
        arrivals = self.parse_flights_table(soup, 'Arribos')
        soup_partidas = self.change_flight_type_and_search(hidden, 'D')
        departures = self.parse_flights_table(soup_partidas, 'Partidas')
        return arrivals, departures

    def filtrar_vuelos_por_posiciones(self, vuelos: List[Dict], posiciones: List[str]) -> List[Dict]:
        posiciones = [p.zfill(2) for p in posiciones]
        return [v for v in vuelos if v.get('Posición', '').strip() in posiciones]

    def scrape_filtered_for_positions(self, posiciones: List[str]) -> List[Dict]:
        arr, dep = self.scrape_all_flights()
        return self.filtrar_vuelos_por_posiciones(arr, posiciones) + self.filtrar_vuelos_por_posiciones(dep, posiciones)

if __name__ == "__main__":
    scraper = TAMSScraperFinal()
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'n8n':
        pos = sys.argv[2:]
        if not pos:
            print("Debe especificar al menos una posición")
            sys.exit(1)
        data = scraper.scrape_filtered_for_positions(pos)
        print(json.dumps(data, ensure_ascii=False))
    else:
        arr, dep = scraper.scrape_all_flights()
        print(json.dumps({'arribos': arr, 'partidas': dep}, ensure_ascii=False))

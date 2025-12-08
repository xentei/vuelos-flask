import requests
from bs4 import BeautifulSoup
import time
import re
import logging
import json
from typing import List, Dict, Tuple
from fake_useragent import UserAgent

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
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        
        # Headers optimizados con compresión
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        })
        
        # Pool optimizado para múltiples requests
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=40,
            max_retries=2,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def extract_viewstate_data(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrae ViewState - OPTIMIZADO"""
        viewstate = {}
        
        # Buscar todos los inputs de una sola vez
        inputs = soup.find_all('input', {'id': ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']})
        
        for field in inputs:
            field_id = field.get('id')
            value = field.get('value')
            if field_id and value:
                viewstate[field_id] = value
                
        return viewstate

    def get_initial_page(self) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Obtiene la página inicial - OPTIMIZADO"""
        logger.info("Obteniendo página inicial...")
        response = self.session.get(self.base_url, timeout=5)
        response.raise_for_status()
        
        # Usar lxml para parsing más rápido (10x faster)
        try:
            soup = BeautifulSoup(response.text, 'lxml')
        except:
            # Fallback a html.parser si lxml no está disponible
            soup = BeautifulSoup(response.text, 'html.parser')
            
        viewstate = self.extract_viewstate_data(soup)
        
        return soup, viewstate

    def make_post_request(self, data: Dict[str, str]) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Método genérico para hacer POST - OPTIMIZADO"""
        response = self.session.post(self.base_url, data=data, timeout=5)
        response.raise_for_status()
        
        # Intentar lxml primero, fallback a html.parser
        try:
            soup = BeautifulSoup(response.text, 'lxml')
        except:
            soup = BeautifulSoup(response.text, 'html.parser')
            
        viewstate = self.extract_viewstate_data(soup)
        
        return soup, viewstate

    def change_to_departures(self, viewstate: Dict[str, str]) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Cambia a partidas - SIN SLEEPS"""
        logger.info("Cambiando a PARTIDAS...")
        
        # Paso 1: Cambiar dropdown
        data = {
            '__EVENTTARGET': 'ddlMovTp',
            '__EVENTARGUMENT': '',
            'ddlMovTp': 'D',
            'ddlAeropuerto': 'AEP',
            'ddlSector': '-1',
            'ddlAerolinea': '-1',
            'ddlAterrizados': 'TODOS',
            'ddlVentanaH': '6',
            **viewstate
        }
        
        soup, viewstate = self.make_post_request(data)
        
        # Paso 2: Buscar
        data.update({
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            'btnBuscar': 'Buscar',
            **viewstate
        })
        
        return self.make_post_request(data)

    def change_time_window_and_search(self, viewstate: Dict[str, str], 
                                       flight_type: str, hours: str) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Cambia ventana horaria - SIN SLEEPS"""
        logger.info(f"Cambiando ventana horaria a {hours}h para {flight_type}...")
        
        # Paso 1: Cambiar dropdown
        data = {
            '__EVENTTARGET': 'ddlVentanaH',
            '__EVENTARGUMENT': '',
            'ddlMovTp': flight_type,
            'ddlAeropuerto': 'AEP',
            'ddlSector': '-1',
            'ddlAerolinea': '-1',
            'ddlAterrizados': 'TODOS',
            'ddlVentanaH': hours,
            **viewstate
        }
        
        soup, viewstate = self.make_post_request(data)
        
        # Paso 2: Buscar
        data.update({
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            'btnBuscar': 'Buscar',
            **viewstate
        })
        
        return self.make_post_request(data)

    def click_pagination(self, viewstate: Dict[str, str], page_target: str, 
                        flight_type: str) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Navega paginación - SIN SLEEPS"""
        page_num = page_target.split('$')[-1]
        logger.info(f"→ Página: {page_num}")
        
        mov_type = 'A' if flight_type == 'Arribos' else 'D'
        
        data = {
            '__EVENTTARGET': page_target,
            '__EVENTARGUMENT': '',
            'ddlMovTp': mov_type,
            'ddlAeropuerto': 'AEP',
            'ddlSector': '-1',
            'ddlAerolinea': '-1',
            'ddlAterrizados': 'TODOS',
            'ddlVentanaH': '6',
            **viewstate
        }
        
        return self.make_post_request(data)

    def parse_flights(self, soup: BeautifulSoup, flight_type: str) -> List[Dict]:
        """Parsea vuelos - OPTIMIZADO"""
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        table = soup.find('table', {'id': table_id})
        
        if not table:
            return []
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return []
        
        # Extraer headers
        headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
        
        # Parseo optimizado
        flights = []
        for row in rows[1:]:
            cells = row.find_all('td')
            
            # Skip paginación
            if len(cells) == 1 and cells[0].get('colspan'):
                continue
            
            if len(cells) >= len(headers):
                flight = {'Tipo': flight_type}
                for idx, cell in enumerate(cells):
                    if idx < len(headers):
                        flight[headers[idx]] = cell.get_text(strip=True)
                flights.append(flight)
        
        return flights

    def get_page_links(self, soup: BeautifulSoup, table_id: str) -> List[str]:
        """Extrae enlaces de paginación - OPTIMIZADO"""
        table = soup.find('table', {'id': table_id})
        if not table:
            return []
        
        pager_row = table.find('tr', {'class': 'Pager'})
        if not pager_row:
            return []
        
        # Extraer todos con regex
        links = []
        for a in pager_row.find_all('a', href=True):
            match = re.search(r"__doPostBack\('([^']+)'", a['href'])
            if match:
                links.append(match.group(1))
        
        return links

    def scrape_all_pages(self, soup: BeautifulSoup, viewstate: Dict[str, str], 
                        flight_type: str, max_pages: int = 3) -> List[Dict]:
        """Scrapea todas las páginas - OPTIMIZADO"""
        all_flights = []
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        
        logger.info(f"\n{'='*70}")
        logger.info(f"SCRAPEANDO {flight_type.upper()}")
        logger.info(f"{'='*70}")
        
        # Página 1
        page1_flights = self.parse_flights(soup, flight_type)
        all_flights.extend(page1_flights)
        
        if page1_flights:
            first = page1_flights[0].get('Vuelo', '?')
            last = page1_flights[-1].get('Vuelo', '?')
            logger.info(f"Pág 1: {len(page1_flights)} vuelos | {first} → {last}")
        else:
            logger.info("Pág 1: Sin vuelos")
        
        # Obtener enlaces
        page_links = self.get_page_links(soup, table_id)
        
        if not page_links:
            logger.info("→ Una sola página disponible")
            return all_flights
        
        logger.info(f"→ Total páginas disponibles: {len(page_links) + 1}")
        
        # Scrapear páginas adicionales
        pages_to_scrape = min(len(page_links), max_pages - 1)
        
        for idx, page_link in enumerate(page_links[:pages_to_scrape]):
            page_num = idx + 2
            
            try:
                soup, viewstate = self.click_pagination(viewstate, page_link, flight_type)
                page_flights = self.parse_flights(soup, flight_type)
                
                if page_flights:
                    # Check duplicados
                    first_new = page_flights[0].get('Vuelo')
                    if any(f.get('Vuelo') == first_new for f in all_flights):
                        logger.warning(f"⚠ Duplicado detectado: {first_new} - Deteniendo paginación")
                        break
                    
                    all_flights.extend(page_flights)
                    first = page_flights[0].get('Vuelo', '?')
                    last = page_flights[-1].get('Vuelo', '?')
                    logger.info(f"Pág {page_num}: {len(page_flights)} vuelos | {first} → {last}")
                else:
                    logger.warning(f"Pág {page_num}: Sin vuelos")
                    
            except Exception as e:
                logger.error(f"Error en página {page_num}: {e}")
                break
        
        logger.info(f"TOTAL {flight_type.upper()}: {len(all_flights)} vuelos")
        logger.info(f"{'='*70}\n")
        
        return all_flights

    def scrape_all_flights(self) -> Tuple[List[Dict], List[Dict]]:
        """Scrapea todo - OPTIMIZADO"""
        start_time = time.time()
        
        # Página inicial (Arribos +6h)
        soup, viewstate = self.get_initial_page()
        arrivals_plus6 = self.scrape_all_pages(soup, viewstate, 'Arribos', max_pages=3)
        
        # Partidas +6h
        soup, viewstate = self.change_to_departures(viewstate)
        departures_plus6 = self.scrape_all_pages(soup, viewstate, 'Partidas', max_pages=3)
        
        # Arribos -1h (solo pág 1)
        logger.info("\n" + "="*70)
        logger.info("CAMBIANDO A VENTANA HORARIA: -1 HORA")
        logger.info("="*70 + "\n")
        
        soup, viewstate = self.change_time_window_and_search(viewstate, 'A', '-1')
        arrivals_minus1 = self.parse_flights(soup, 'Arribos')
        logger.info(f"Arribos -1h: {len(arrivals_minus1)} vuelos\n")
        
        # Partidas -1h (solo pág 1)
        soup, viewstate = self.change_time_window_and_search(viewstate, 'D', '-1')
        departures_minus1 = self.parse_flights(soup, 'Partidas')
        logger.info(f"Partidas -1h: {len(departures_minus1)} vuelos\n")
        
        # Combinar resultados
        all_arrivals = arrivals_plus6 + arrivals_minus1
        all_departures = departures_plus6 + departures_minus1
        
        elapsed = time.time() - start_time
        
        logger.info("="*70)
        logger.info(f"RESUMEN FINAL - Tiempo: {elapsed:.2f}s")
        logger.info(f"  Arribos: {len(all_arrivals)} ({len(arrivals_plus6)} +6h, {len(arrivals_minus1)} -1h)")
        logger.info(f"  Partidas: {len(all_departures)} ({len(departures_plus6)} +6h, {len(departures_minus1)} -1h)")
        logger.info(f"  TOTAL: {len(all_arrivals) + len(all_departures)} vuelos")
        logger.info(f"  Velocidad: {(len(all_arrivals) + len(all_departures)) / elapsed:.1f} vuelos/seg")
        logger.info("="*70)
        
        return all_arrivals, all_departures

    def filtrar_vuelos_por_posiciones(self, vuelos: List[Dict], posiciones: List[str]) -> List[Dict]:
        """Filtro optimizado con set"""
        posiciones_set = {p.zfill(2) for p in posiciones}
        return [v for v in vuelos if v.get('Posición', '').strip() in posiciones_set]

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

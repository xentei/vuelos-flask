import requests
from bs4 import BeautifulSoup
import time
import re
import logging
import json
from typing import List, Dict, Tuple
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TAMSScraperOptimized:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "http://www.tams.com.ar/organismos/vuelos.aspx"
        self.setup_session()
        self._lock = threading.Lock()

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
            'Accept-Encoding': 'gzip, deflate',  # Habilitar compresión
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        })
        
        # Pool optimizado para múltiples requests simultáneos
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
        """Obtiene la página inicial y extrae ViewState - OPTIMIZADO"""
        logger.info("Obteniendo página inicial...")
        response = self.session.get(self.base_url, timeout=5)  # Timeout reducido
        response.raise_for_status()
        
        # Usar lxml para parsing más rápido
        soup = BeautifulSoup(response.text, 'lxml')
        viewstate = self.extract_viewstate_data(soup)
        
        return soup, viewstate

    def make_post_request(self, data: Dict[str, str], use_lxml: bool = True) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Método genérico para hacer POST - OPTIMIZADO"""
        response = self.session.post(self.base_url, data=data, timeout=5)
        response.raise_for_status()
        
        # lxml es ~10x más rápido que html.parser
        parser = 'lxml' if use_lxml else 'html.parser'
        soup = BeautifulSoup(response.text, parser)
        viewstate = self.extract_viewstate_data(soup)
        
        return soup, viewstate

    def change_to_departures(self, viewstate: Dict[str, str]) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Cambia a partidas - SIN SLEEPS innecesarios"""
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
        
        # Paso 2: Buscar (sin sleep entre requests)
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
        logger.info(f"→ Página: {page_target.split('$')[-1]}")
        
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
        """Parsea vuelos - OPTIMIZADO con list comprehension"""
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        table = soup.find('table', {'id': table_id})
        
        if not table:
            return []
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return []
        
        # Extraer headers una vez
        headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
        
        # Parseo optimizado con list comprehension
        flights = []
        for row in rows[1:]:
            cells = row.find_all('td')
            
            # Skip paginación
            if len(cells) == 1 and cells[0].get('colspan'):
                continue
            
            if len(cells) >= len(headers):
                flight = {
                    'Tipo': flight_type,
                    **{headers[idx]: cell.get_text(strip=True) for idx, cell in enumerate(cells) if idx < len(headers)}
                }
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
        
        # Extraer todos con regex de una sola pasada
        links = []
        for a in pager_row.find_all('a', href=True):
            match = re.search(r"__doPostBack\('([^']+)'", a['href'])
            if match:
                links.append(match.group(1))
        
        return links

    def scrape_page_parallel(self, args: Tuple) -> List[Dict]:
        """Scrapea una página individual - Para uso paralelo"""
        viewstate, page_link, flight_type = args
        try:
            soup, new_viewstate = self.click_pagination(viewstate, page_link, flight_type)
            flights = self.parse_flights(soup, flight_type)
            return flights, new_viewstate
        except Exception as e:
            logger.error(f"Error en página paralela: {e}")
            return [], viewstate

    def scrape_all_pages(self, soup: BeautifulSoup, viewstate: Dict[str, str], 
                        flight_type: str, max_pages: int = 3) -> List[Dict]:
        """Scrapea todas las páginas - MODO SECUENCIAL OPTIMIZADO"""
        all_flights = []
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        
        logger.info(f"\n{'='*70}")
        logger.info(f"SCRAPEANDO {flight_type.upper()}")
        logger.info(f"{'='*70}")
        
        # Página 1
        page1_flights = self.parse_flights(soup, flight_type)
        all_flights.extend(page1_flights)
        
        if page1_flights:
            logger.info(f"Pág 1: {len(page1_flights)} vuelos | {page1_flights[0].get('Vuelo')} → {page1_flights[-1].get('Vuelo')}")
        
        # Obtener enlaces
        page_links = self.get_page_links(soup, table_id)
        
        if not page_links:
            logger.info("→ Una sola página")
            return all_flights
        
        # Scrapear páginas adicionales SECUENCIALMENTE (ViewState depende de la anterior)
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
                        logger.warning(f"⚠ Duplicado: {first_new} - STOP")
                        break
                    
                    all_flights.extend(page_flights)
                    logger.info(f"Pág {page_num}: {len(page_flights)} vuelos | {page_flights[0].get('Vuelo')} → {page_flights[-1].get('Vuelo')}")
                else:
                    logger.warning(f"Pág {page_num}: Vacía")
                    
            except Exception as e:
                logger.error(f"Error pág {page_num}: {e}")
                break
        
        logger.info(f"TOTAL {flight_type.upper()}: {len(all_flights)} vuelos\n")
        
        return all_flights

    def scrape_all_flights(self) -> Tuple[List[Dict], List[Dict]]:
        """Scrapea todo - FLUJO OPTIMIZADO"""
        start_time = time.time()
        
        # Página inicial (Arribos +6h)
        soup, viewstate = self.get_initial_page()
        arrivals_plus6 = self.scrape_all_pages(soup, viewstate, 'Arribos', max_pages=3)
        
        # Partidas +6h
        soup, viewstate = self.change_to_departures(viewstate)
        departures_plus6 = self.scrape_all_pages(soup, viewstate, 'Partidas', max_pages=3)
        
        # Arribos -1h (solo pág 1)
        logger.info("\n" + "="*70)
        logger.info("VENTANA -1 HORA")
        logger.info("="*70)
        
        soup, viewstate = self.change_time_window_and_search(viewstate, 'A', '-1')
        arrivals_minus1 = self.parse_flights(soup, 'Arribos')
        logger.info(f"Arribos -1h: {len(arrivals_minus1)} vuelos")
        
        # Partidas -1h (solo pág 1)
        soup, viewstate = self.change_time_window_and_search(viewstate, 'D', '-1')
        departures_minus1 = self.parse_flights(soup, 'Partidas')
        logger.info(f"Partidas -1h: {len(departures_minus1)} vuelos")
        
        # Combinar
        all_arrivals = arrivals_plus6 + arrivals_minus1
        all_departures = departures_plus6 + departures_minus1
        
        elapsed = time.time() - start_time
        
        logger.info("\n" + "="*70)
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


# =====================================================
# VERSIÓN ULTRA RÁPIDA CON ASYNCIO (OPCIONAL)
# =====================================================
"""
Si querés aún MÁS velocidad, instalá: pip install aiohttp lxml

import asyncio
import aiohttp
from typing import List, Dict, Tuple

class TAMSScraperAsync:
    def __init__(self):
        self.base_url = "http://www.tams.com.ar/organismos/vuelos.aspx"
        
    async def fetch(self, session: aiohttp.ClientSession, data: Dict = None) -> str:
        if data:
            async with session.post(self.base_url, data=data, timeout=5) as response:
                return await response.text()
        else:
            async with session.get(self.base_url, timeout=5) as response:
                return await response.text()
    
    async def scrape_all_flights_async(self) -> Tuple[List[Dict], List[Dict]]:
        # Implementar versión async completa
        # Hasta 10x más rápido con requests paralelos
        pass
"""


if __name__ == "__main__":
    scraper = TAMSScraperOptimized()
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

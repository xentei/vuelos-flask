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
        
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def extract_viewstate_data(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrae los tres campos críticos de ASP.NET ViewState"""
        viewstate = {}
        
        for field_name in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
            field = soup.find('input', {'id': field_name})
            if field and field.get('value'):
                viewstate[field_name] = field.get('value')
            else:
                logger.error(f"CRÍTICO: No se encontró {field_name}")
                
        return viewstate

    def get_initial_page(self) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Obtiene la página inicial y extrae ViewState"""
        logger.info("Obteniendo página inicial...")
        response = self.session.get(self.base_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        viewstate = self.extract_viewstate_data(soup)
        
        return soup, viewstate

    def change_to_departures(self, viewstate: Dict[str, str]) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """Cambia a partidas con el proceso completo de ASP.NET"""
        logger.info("Cambiando a PARTIDAS...")
        
        # Paso 1: Cambiar el dropdown (esto dispara un postback)
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
        
        response = self.session.post(self.base_url, data=data)
        soup = BeautifulSoup(response.text, 'html.parser')
        viewstate = self.extract_viewstate_data(soup)
        time.sleep(1)
        
        # Paso 2: Hacer clic en Buscar
        data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            'ddlMovTp': 'D',
            'ddlAeropuerto': 'AEP',
            'ddlSector': '-1',
            'ddlAerolinea': '-1',
            'ddlAterrizados': 'TODOS',
            'ddlVentanaH': '6',
            'btnBuscar': 'Buscar',
            **viewstate
        }
        
        response = self.session.post(self.base_url, data=data)
        soup = BeautifulSoup(response.text, 'html.parser')
        viewstate = self.extract_viewstate_data(soup)
        time.sleep(1)
        
        return soup, viewstate

    def click_pagination(self, viewstate: Dict[str, str], page_target: str, 
                        flight_type: str) -> Tuple[BeautifulSoup, Dict[str, str]]:
        """
        Hace clic en un enlace de paginación.
        CLAVE: En ASP.NET DataGrid, hacer clic en paginación NO requiere btnBuscar
        """
        logger.info(f"Navegando a: {page_target}")
        
        # Determinar el valor de ddlMovTp según el tipo
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
        
        # NO incluir btnBuscar aquí - solo para búsquedas, no para paginación
        
        response = self.session.post(self.base_url, data=data)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extraer nuevo ViewState INMEDIATAMENTE
        new_viewstate = self.extract_viewstate_data(soup)
        
        time.sleep(1)
        
        return soup, new_viewstate

    def parse_flights(self, soup: BeautifulSoup, flight_type: str) -> List[Dict]:
        """Extrae vuelos de la tabla"""
        flights = []
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        table = soup.find('table', {'id': table_id})
        
        if not table:
            return flights
        
        rows = table.find_all('tr')
        if len(rows) < 2:
            return flights
        
        headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
        
        for row in rows[1:]:
            cells = row.find_all('td')
            
            # Saltar fila de paginación
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
        """Extrae enlaces de paginación del DataGrid"""
        links = []
        table = soup.find('table', {'id': table_id})
        
        if not table:
            return links
        
        # Buscar la fila con class="Pager"
        pager_row = table.find('tr', {'class': 'Pager'})
        if not pager_row:
            return links
        
        # Extraer todos los enlaces
        for a in pager_row.find_all('a'):
            href = a.get('href', '')
            match = re.search(r"__doPostBack\('([^']+)'", href)
            if match:
                target = match.group(1)
                links.append(target)
        
        return links

    def scrape_all_pages(self, soup: BeautifulSoup, viewstate: Dict[str, str], 
                        flight_type: str, max_pages: int = 3) -> List[Dict]:
        """Scrapea todas las páginas de un tipo de vuelo"""
        all_flights = []
        table_id = 'dgGrillaA' if flight_type == 'Arribos' else 'dgGrillaD'
        
        logger.info(f"\n{'='*70}")
        logger.info(f"SCRAPEANDO {flight_type.upper()}")
        logger.info(f"{'='*70}")
        
        # Página 1
        logger.info("Página 1...")
        page1_flights = self.parse_flights(soup, flight_type)
        all_flights.extend(page1_flights)
        
        if page1_flights:
            logger.info(f"  ✓ {len(page1_flights)} vuelos")
            logger.info(f"    Primero: {page1_flights[0].get('Vuelo')} - {page1_flights[0].get('Origen', page1_flights[0].get('Destino'))}")
            logger.info(f"    Último: {page1_flights[-1].get('Vuelo')} - {page1_flights[-1].get('Origen', page1_flights[-1].get('Destino'))}")
        
        # Obtener enlaces de paginación
        page_links = self.get_page_links(soup, table_id)
        
        if not page_links:
            logger.info("  → No hay más páginas")
            return all_flights
        
        logger.info(f"  → Páginas disponibles: {len(page_links) + 1}")
        
        # Scrapear páginas adicionales
        pages_to_scrape = min(len(page_links), max_pages - 1)
        
        for idx, page_link in enumerate(page_links[:pages_to_scrape]):
            page_num = idx + 2
            logger.info(f"\nPágina {page_num}...")
            logger.info(f"  Target: {page_link}")
            
            try:
                # Navegar a la página
                soup, viewstate = self.click_pagination(viewstate, page_link, flight_type)
                
                # Parsear vuelos
                page_flights = self.parse_flights(soup, flight_type)
                
                if page_flights:
                    # Verificar duplicados
                    first_new = page_flights[0].get('Vuelo')
                    is_duplicate = any(f.get('Vuelo') == first_new for f in all_flights)
                    
                    if is_duplicate:
                        logger.warning(f"  ⚠ DUPLICADO DETECTADO - vuelo {first_new} ya existe")
                        logger.warning(f"  → Deteniendo paginación")
                        break
                    
                    all_flights.extend(page_flights)
                    logger.info(f"  ✓ {len(page_flights)} vuelos")
                    logger.info(f"    Primero: {page_flights[0].get('Vuelo')} - {page_flights[0].get('Origen', page_flights[0].get('Destino'))}")
                    logger.info(f"    Último: {page_flights[-1].get('Vuelo')} - {page_flights[-1].get('Origen', page_flights[-1].get('Destino'))}")
                else:
                    logger.warning(f"  ⚠ No se encontraron vuelos")
                    
            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                break
        
        logger.info(f"\n{'='*70}")
        logger.info(f"TOTAL {flight_type.upper()}: {len(all_flights)} vuelos")
        logger.info(f"{'='*70}\n")
        
        return all_flights

    def scrape_all_flights(self) -> Tuple[List[Dict], List[Dict]]:
        """Scrapea arribos y partidas"""
        # Obtener página inicial (viene con Arribos por defecto)
        soup, viewstate = self.get_initial_page()
        
        # Scrapear ARRIBOS
        arrivals = self.scrape_all_pages(soup, viewstate, 'Arribos', max_pages=3)
        
        # Cambiar a PARTIDAS
        soup, viewstate = self.change_to_departures(viewstate)
        
        # Scrapear PARTIDAS
        departures = self.scrape_all_pages(soup, viewstate, 'Partidas', max_pages=3)
        
        logger.info("="*70)
        logger.info(f"RESUMEN FINAL")
        logger.info(f"  Arribos: {len(arrivals)}")
        logger.info(f"  Partidas: {len(departures)}")
        logger.info(f"  Total: {len(arrivals) + len(departures)}")
        logger.info("="*70)
        
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

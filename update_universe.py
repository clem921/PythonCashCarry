import os
import subprocess
import pandas as pd
import requests
import time
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from universe_manager import UniverseDatabase
import config

INPUTS_DIR = "inputs"

# Constants for API endpoints and headers
EURONEXT_BASE_URL = "https://live.euronext.com"
EURONEXT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://live.euronext.com/en/products/equities/list',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'application/json, text/javascript, */*; q=0.01'
}

EUREX_BASE_URL = "https://www.eurex.com"
EUREX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

def get_euronext_equities_map():
    """
    AMÉLIORÉ: Construit une table ISIN -> Ticker pour les actions Euronext.
    """
    print("Mise à jour de la carte ISIN -> Ticker (Actions)...")
    isin_ticker_map = {}
    session = requests.Session()
    session.headers.update(EURONEXT_HEADERS)
    
    for page in range(50): # Suffisant pour ~2500 instruments
        url = f"https://live.euronext.com/en/products/equities/list?page={page}"
        try:
            response = session.get(url, timeout=15)
            if response.status_code != 200: break
                
            html = response.text
            # Utiliser une regex robuste (insensible à la casse, gère les attributs)
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.S | re.I)
            if not rows or len(rows) < 5: break
                
            found_on_page = 0
            for row in rows:
                if '<th' in row.lower() and 'priority-' not in row.lower(): continue # Skip real headers
                # Chercher th ou td pour les cellules
                tds = re.findall(r'<(?:td|th)[^>]*>(.*?)</(?:td|th)>', row, re.S | re.I)
                if len(tds) >= 3:
                    # En Equities: Col 1: Ticker, Col 2: ISIN
                    ticker = re.sub(r'<[^>]+>', '', tds[1]).strip()
                    isin = re.sub(r'<[^>]+>', '', tds[2]).strip()
                    
                    if len(isin) == 12 and isin[0:2].isalpha() and ticker:
                        isin_ticker_map[isin] = ticker
                        found_on_page += 1
            
            print(f"  Page {page + 1}: {found_on_page} actions mappées", end='\r')
            if 'page=' + str(page + 1) not in html.lower(): break
            time.sleep(0.1)
        except: break
            
    print(f"\nCarte prête: {len(isin_ticker_map)} tickers enregistrés.")
    return isin_ticker_map

def get_euronext_metadata(isin, mic='XPAR'):
    """
    AMÉLIORÉ: Extrait le ticker (symbol) et le nom complet depuis la page produit Euronext via le bloc JSON.
    C'est la méthode la plus robuste car elle évite le parsing HTML fragile.
    """
    url = f"https://live.euronext.com/en/product/equities/{isin}-{mic}"
    try:
        response = requests.get(url, headers=EURONEXT_HEADERS, timeout=10)
        if response.status_code == 200:
            html = response.text
            # Chercher le bloc drupal-settings-json
            match = re.search(r'<script type="application/json" data-drupal-selector="drupal-settings-json">(.*?)</script>', html, re.S)
            if match:
                data = json.loads(match.group(1))
                instr_info = data.get('custom', {}).get('instrument', {})
                if instr_info:
                    return {
                        'ticker': instr_info.get('symbol'),
                        'name': instr_info.get('name'),
                        'isin': instr_info.get('isin'),
                        'mic': instr_info.get('mic')
                    }
    except Exception as e:
        pass
    return None

def get_euronext_expirations(future_symbol, exchange, category="stock-futures"):
    """
    Récupérer les dates d'expiration pour un future donné via l'API AJAX getPricesFutures.
    Retourne une liste de dates au format DD/MM/YYYY.
    """
    ajax_headers = {
        'User-Agent': EURONEXT_HEADERS['User-Agent'],
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': '*/*',
        'Referer': f'https://live.euronext.com/en/product/{category}/{future_symbol}-{exchange}',
    }
    url = f"https://live.euronext.com/en/ajax/getPricesFutures/{category}/{future_symbol}/{exchange}"
    try:
        response = requests.get(url, headers=ajax_headers, timeout=10)
        if response.status_code == 200:
            # Les dates sont dans les paramètres md=DD-MM-YYYY des liens
            md_dates = re.findall(r'md=(\d{2}-\d{2}-\d{4})', response.text)
            if md_dates:
                # Convertir DD-MM-YYYY en DD/MM/YYYY pour cohérence
                dates = [d.replace('-', '/') for d in md_dates]
                return sorted(list(set(dates)))
    except:
        pass
    return []


def get_euronext_underlying_info(future_symbol, exchange):
    """
    Récupérer les informations du sous-jacent via l'API AJAX getUnderlying.
    Retourne un dict avec name, isin, market ou None.
    """
    ajax_headers = {
        'User-Agent': EURONEXT_HEADERS['User-Agent'],
        'X-Requested-With': 'XMLHttpRequest',
        'Accept': '*/*',
        'Referer': f'https://live.euronext.com/en/product/stock-futures/{future_symbol}-{exchange}',
    }
    url = f"https://live.euronext.com/en/ajax/getUnderlying/{future_symbol}/{exchange}/futures"
    try:
        response = requests.get(url, headers=ajax_headers, timeout=10)
        if response.status_code == 200:
            html = response.text
            # Extraire les paires label/valeur du HTML
            name_match = re.search(r'Name.*?<td[^>]*>\s*(\S[^<]*\S)\s*</td>', html, re.S)
            isin_match = re.search(r'ISIN.*?<td[^>]*>\s*([A-Z]{2}[A-Z0-9]{10})\s*</td>', html, re.S)
            market_match = re.search(r'Market.*?<td[^>]*>\s*(\S[^<]*\S)\s*</td>', html, re.S)
            
            return {
                'name': name_match.group(1).strip() if name_match else '',
                'isin': isin_match.group(1).strip() if isin_match else '',
                'market': market_match.group(1).strip() if market_match else '',
            }
    except:
        pass
    return None


# Mapping MIC (ISO 10383) vers IBKR primaryExchange
_MIC_TO_IBKR_EXCHANGE = {
    'XPAR': 'SBF',    # Euronext Paris
    'XAMS': 'AEB',    # Euronext Amsterdam
    'XBRU': 'EBR',    # Euronext Brussels
    'XLIS': 'ENXL',   # Euronext Lisbon
    'XMIL': 'BVME',   # Borsa Italiana (Milan)
    'XETR': 'IBIS',   # Xetra (Francfort)
    'XLON': 'LSE',    # London Stock Exchange
    'XSWX': 'EBS',    # SIX Swiss Exchange
    'XOSL': 'OSE',    # Oslo Børs
    'XHEL': 'HEX',    # Helsinki (Nasdaq Nordic)
    'XCSE': 'CSE',    # Copenhagen (Nasdaq Nordic)
    'XSTO': 'SFB',    # Stockholm (Nasdaq Nordic)
    'XWBO': 'VSE',    # Vienna Stock Exchange
    'XDUB': 'ISE',    # Irish Stock Exchange
    'XMAD': 'BM',     # Bolsa de Madrid
}

# Mapping OpenFIGI exchCode vers IBKR primaryExchange
_OPENFIGI_TO_IBKR_EXCHANGE = {
    'FP': 'SBF', 'PA': 'SBF', 'EN': 'SBF',
    'NA': 'AEB', 'AS': 'AEB',
    'BB': 'EBR', 'BT': 'EBR',
    'PL': 'ENXL',
    'GR': 'IBIS', 'GY': 'IBIS', 'TE': 'IBIS', 'DB': 'IBIS',
    'IM': 'BVME', 'MI': 'BVME',
    'LN': 'LSE', 'LS': 'LSE',
    'VX': 'EBS', 'SE': 'EBS', 'SW': 'EBS',
    'NO': 'OSE', 'OL': 'OSE',
    'SM': 'BM', 'MC': 'BM',
    'FH': 'HEX', 'HE': 'HEX',
    'DC': 'CSE', 'CO': 'CSE',
    'SS': 'SFB', 'ST': 'SFB',
    'AV': 'VSE', 'VI': 'VSE',
    'ID': 'ISE', 'IR': 'ISE',
}


# Mapping pays ISIN → IBKR primaryExchange (fallback quand seul l'ISIN est disponible)
_ISIN_COUNTRY_TO_IBKR_EXCHANGE = {
    'FR': 'SBF',    # Euronext Paris
    'NL': 'AEB',    # Euronext Amsterdam
    'BE': 'EBR',    # Euronext Brussels
    'PT': 'ENXL',   # Euronext Lisbon
    'DE': 'IBIS',   # Xetra
    'IT': 'BVME',   # Borsa Italiana
    'GB': 'LSE',    # London
    'CH': 'EBS',    # SIX Swiss
    'NO': 'OSE',    # Oslo
    'ES': 'BM',     # Madrid
    'FI': 'HEX',    # Helsinki
    'DK': 'CSE',    # Copenhagen
    'SE': 'SFB',    # Stockholm
    'AT': 'VSE',    # Vienna
    'IE': 'ISE',    # Dublin
}


def get_underlying_ticker_from_equity_page(isin):
    """
    Récupérer le ticker et la place de cotation du sous-jacent via la page equity Euronext (drupal JSON).
    Fonctionne pour les ISIN listés sur Euronext (FR, NL, BE, PT, certains NO).
    Retourne (ticker, primary_exchange) ou ('', '').
    """
    # Ne tenter que les MICs cohérents avec le pays de l'ISIN
    _ISIN_COUNTRY_MICS = {
        'FR': ['XPAR'],
        'NL': ['XAMS'],
        'BE': ['XBRU'],
        'PT': ['XLIS'],
        'NO': ['XOSL'],
        'IT': ['XMIL'],
        'IE': ['XDUB'],
    }
    country = isin[:2] if len(isin) >= 2 else ''
    mics = _ISIN_COUNTRY_MICS.get(country, [])
    if not mics:
        return '', ''

    for mic in mics:
        url = f"https://live.euronext.com/en/product/equities/{isin}-{mic}"
        try:
            response = requests.get(url, headers=EURONEXT_HEADERS, timeout=8)
            if response.status_code == 200:
                match = re.search(r'data-drupal-selector="drupal-settings-json">(.*?)</script>', response.text, re.S)
                if match:
                    data = json.loads(match.group(1))
                    instr = data.get('custom', {}).get('instrument', {})
                    symbol = instr.get('symbol', '')
                    if symbol:
                        resolved_mic = instr.get('mic', mic)
                        primary_exchange = _MIC_TO_IBKR_EXCHANGE.get(resolved_mic, '')
                        return symbol, primary_exchange
        except:
            continue
    return '', ''


# Priorité des exchanges OpenFIGI par pays d'ISIN
_OPENFIGI_EXCHANGE_PRIORITY = {
    'FR': ['FP', 'PA', 'EN'],
    'NL': ['NA', 'AS', 'EN'],
    'BE': ['BB', 'BT', 'EN'],
    'PT': ['PL', 'EN'],
    'DE': ['GR', 'GY', 'TE', 'DB'],
    'IT': ['IM', 'MI'],
    'GB': ['LN', 'LS'],
    'CH': ['VX', 'SE', 'SW'],
    'NO': ['NO', 'OL'],
    'ES': ['SM', 'MC'],
    'FI': ['FH', 'HE'],
    'DK': ['DC', 'CO'],
    'SE': ['SS', 'ST'],
    'AT': ['AV', 'VI'],
    'IE': ['ID', 'IR'],
}


def get_ticker_from_openfigi(isin):
    """
    Récupérer le ticker et la place de cotation via l'API OpenFIGI (gratuit, pas de clé).
    Privilégie l'exchange principal du pays de l'ISIN.
    Gère le rate-limiting (429) avec retry automatique.
    Retourne (ticker, primary_exchange) ou ('', '').
    """
    url = 'https://api.openfigi.com/v3/mapping'
    payload = [{'idType': 'ID_ISIN', 'idValue': isin}]

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload,
                                 headers={'Content-Type': 'application/json'}, timeout=10)
            if resp.status_code == 429:
                wait = 7 * (attempt + 1)
                print(f"    [OpenFIGI] Rate limit atteint pour {isin}, attente {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                return '', ''
            data = resp.json()
            if data and data[0].get('data'):
                results = data[0]['data']
                equities = [r for r in results
                            if r.get('marketSector') == 'Equity' and r.get('ticker')]
                if not equities:
                    equities = [r for r in results if r.get('ticker')]
                if not equities:
                    return '', ''

                country = isin[:2]
                preferred = _OPENFIGI_EXCHANGE_PRIORITY.get(country, [])
                for pref in preferred:
                    for r in equities:
                        if r.get('exchCode') == pref:
                            return r['ticker'], _OPENFIGI_TO_IBKR_EXCHANGE.get(pref, '')

                # Fallback : premier résultat non-US
                for r in equities:
                    if r.get('exchCode') not in ('US', 'UA', 'UW', 'UN', 'UQ'):
                        return r['ticker'], _OPENFIGI_TO_IBKR_EXCHANGE.get(r['exchCode'], '')
                return equities[0]['ticker'], _OPENFIGI_TO_IBKR_EXCHANGE.get(equities[0].get('exchCode', ''), '')
        except Exception as e:
            print(f"    [OpenFIGI] Erreur pour {isin}: {e}")
            break
    return '', ''


def resolve_underlying_ticker(isin, isin_ticker_cache):
    """
    Résoudre l'ISIN d'un sous-jacent vers son ticker boursier et sa place de cotation.
    Cascade : cache → page equity Euronext → OpenFIGI.
    Retourne (ticker, primary_exchange).
    """
    if not isin or len(isin) != 12:
        return '', ''

    # 1. Cache
    if isin in isin_ticker_cache:
        cached = isin_ticker_cache[isin]
        return cached

    # 2. Page equity Euronext (FR, NL, BE, PT)
    ticker, primary_exchange = get_underlying_ticker_from_equity_page(isin)

    # 3. Fallback OpenFIGI (DE, IT, GB, CH, etc.)
    if not ticker:
        ticker, primary_exchange = get_ticker_from_openfigi(isin)
        time.sleep(6)  # Rate limit OpenFIGI (10 req/min sans clé API)

    # LSE utilise '/' dans les tickers (AV/, BP/, BT/A) mais IBKR utilise '.'
    if ticker and '/' in ticker:
        ticker = ticker.replace('/', '.')

    isin_ticker_cache[isin] = (ticker, primary_exchange)
    return ticker, primary_exchange

def download_euronext_data(categories=None):
    """
    Télécharger exhaustivement les futures Euronext, enrichir chaque contrat avec :
    - ticker du future, nom du future
    - ticker et nom du sous-jacent (via API AJAX getUnderlying + page equity)
    - dates d'expiration (via API AJAX getPricesFutures)
    
    Args:
        categories: Liste des catégories à scraper (défaut: toutes)
                   - 'stock-futures' : Single Stock Futures (SSF)
                   - 'index-futures' : Futures sur indices
                   - 'dividend-stock-futures' : Dividend Stock Futures
    """
    print("\n=== DÉMARRAGE ENRICHISSEMENT EURONEXT ===")
    db = UniverseDatabase()
    
    # Désactiver tous les anciens assets Euronext avant mise à jour
    # Les assets trouvés seront réactivés automatiquement (active=1 dans l'upsert)
    print("  Désactivation des anciens assets SCRAPER_EURONEXT...")
    db.deactivate_all_from_source('SCRAPER_EURONEXT')
    
    # Catégories par défaut si non spécifiées
    if categories is None:
        categories = ["stock-futures", "index-futures", "dividend-stock-futures"]
    session = requests.Session()
    session.headers.update(EURONEXT_HEADERS)
    
    mic_map = {'Paris': 'DPAR', 'Amsterdam': 'DAMS', 'Brussels': 'XBRU', 'Lisbon': 'XLIS',
               'Dublin': 'XDUB', 'Oslo': 'XOSL', 'Milan': 'DMIL'}
    
    # Cache ISIN -> ticker pour éviter les appels répétés
    isin_ticker_cache = {}
    
    total_added = 0
    total_enriched = 0
    
    for category in categories:
        page = 0
        print(f"\nScraping catégorie: {category}")
        
        while True:
            url = f"https://live.euronext.com/en/products/{category}/list?page={page}"
            print(f"  Page {page + 1}...", end=' ', flush=True)
            
            try:
                response = session.get(url, timeout=15)
                if response.status_code != 200: break
                
                html = response.text
                
                # Trouver la table des produits
                table_match = re.search(r'<table[^>]*class=[^>]*table[^>]*>(.*?)</table>', html, re.S | re.I)
                if not table_match:
                    print("Pas de table trouvée. Fin.")
                    break
                
                rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_match.group(1), re.S | re.I)
                if not rows or len(rows) < 2:
                    print("Fin.")
                    break
                
                found_on_page = 0
                for row in rows:
                    # Ignorer les headers
                    if '<th' in row.lower() and 'priority-' not in row.lower():
                        continue
                    
                    # 1. Extraire PID via le lien produit (ex: EA6-DPAR)
                    pid_match = re.search(r'/en/product/[^/]+/([A-Z0-9]+-[A-Z]+)', row, re.I)
                    if not pid_match:
                        continue
                    pid = pid_match.group(1)
                    parts = pid.split('-')
                    future_ticker = parts[0]
                    future_exchange = parts[1] if len(parts) > 1 else ''
                    
                    # 2. Extraire les cellules TD
                    tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S | re.I)
                    if len(tds) < 3:
                        continue
                    
                    # 3. Nom du future (1er TD, nettoyer HTML et suffixe)
                    future_name = re.sub(r'<[^>]+>', '', tds[0]).strip()
                    # Garder le nom complet tel quel
                    
                    # 4. Code du future (2ème TD)
                    future_code = re.sub(r'<[^>]+>', '', tds[1]).strip()
                    
                    # 5. ISIN du sous-jacent (4ème TD si présent)
                    underlying_isin = ""
                    if len(tds) >= 4:
                        isin_match = re.search(r'([A-Z]{2}[A-Z0-9]{10})', tds[3])
                        if isin_match:
                            underlying_isin = isin_match.group(1)
                    
                    # 6. Localisation / MIC
                    location = re.sub(r'<[^>]+>', '', tds[2]).strip() if len(tds) >= 3 else ''
                    derivatives_exchange = future_exchange
                    
                    # 7. Nom du sous-jacent (nettoyer le suffixe "- Stock Future")
                    underlying_name = re.sub(r'\s*-\s*(Stock|Index|Dividend|Single).*$', '', future_name, flags=re.I).strip()
                    
                    # 8. Toujours appeler getUnderlying pour compléter ISIN + nom
                    und_info = get_euronext_underlying_info(future_ticker, future_exchange)
                    if und_info:
                        if not underlying_isin and und_info.get('isin'):
                            underlying_isin = und_info['isin']
                        if und_info.get('name'):
                            underlying_name = und_info['name']
                        time.sleep(0.1)
                    
                    # 9. Résoudre le ticker et la place de cotation du sous-jacent (Euronext → OpenFIGI)
                    underlying_ticker, underlying_primary_exchange = resolve_underlying_ticker(underlying_isin, isin_ticker_cache)
                    if underlying_ticker:
                        total_enriched += 1
                    
                    # Déterminer underlying_symbol : ticker > ISIN > jamais le nom
                    resolved_underlying = underlying_ticker or underlying_isin
                    if not resolved_underlying:
                        print(f"    [WARN] {future_ticker}: pas de ticker ni ISIN pour le sous-jacent")
                    
                    # 10. Expirations via API AJAX
                    expirations = get_euronext_expirations(future_ticker, future_exchange, category)
                    time.sleep(0.1)
                    
                    success = db.add_future_asset(
                        symbol=future_ticker,
                        future_symbol=future_ticker,
                        name=future_name,
                        underlying_symbol=resolved_underlying,
                        exchange=derivatives_exchange or 'SMART',
                        currency='EUR',
                        source='SCRAPER_EURONEXT',
                        derivatives_exchange=derivatives_exchange,
                        underlying_primary_exchange=underlying_primary_exchange,
                        multiplier=1,
                        expirations=",".join(expirations),
                        underlying_isin=underlying_isin,
                        derivative_category=category
                    )
                    
                    if success:
                        found_on_page += 1
                        total_added += 1
                
                print(f"{found_on_page} intégrés.")
                if 'page=' + str(page + 1) not in html.lower():
                    break
                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"Erreur: {e}")
                break

    print(f"\n=== RÉSUMÉ EURONEXT ENRICHI ===")
    print(f"Total contrats: {total_added} | Sous-jacents enrichis (ticker trouvé): {total_enriched}")
    return True

# Les fonctions legacy import_from_files and extract_euronext_product_links ont été supprimées
# car l'intégration est maintenant directe en base de données.

def _fetch_url_content(url):
    """
    Fetch URL content using curl via subprocess to bypass potential blocking of python-requests.
    Returns the decoded HTML content or None on failure.
    """
    try:
        # Utiliser curl pour imiter un vrai navigateur
        result = subprocess.run(
            ['curl.exe', '-s', '-L', 
             '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
             url],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=20
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
        print(f"    Curl failed for {url}. Code: {result.returncode}, Stderr: {result.stderr[:500]}")
        return None
    except Exception as e:
        print(f"    Erreur curl {url}: {e}")
        return None

import gzip
import io
import xml.etree.ElementTree as ET
import os

def _get_product_urls_from_sitemap():
    """
    Récupérer les URLs produits depuis le sitemap Eurex (plus robuste que le listing).
    1. Robots.txt -> sitemap.xml
    2. sitemap.xml -> xxxxx.xml.gz
    3. Gunzip -> extraction des URLs produits
    """
    print("  Récupération via Sitemap...")
    products = []
    
    # 1. Sitemap Index
    sitemap_url = "https://www.eurex.com/sitemap.xml"
    content = _fetch_url_content(sitemap_url)
    if not content:
        print("    Impossible de lire sitemap.xml")
        return products
    
    # Trouver les sous-sitemaps
    # Pattern simple: <loc>...xml.gz</loc>
    sub_sitemaps = re.findall(r'<loc>([^<]+\.xml\.gz)</loc>', content)
    print(f"    Trouvé {len(sub_sitemaps)} sous-sitemaps.")
    
    for sub_url in sub_sitemaps:
        print(f"    Processing {sub_url.split('/')[-1]}...")
        
        # Télécharger le .gz
        # curl pour récupérer le binaire
        try:
            result = subprocess.run(
                ['curl.exe', '-s', '-L', sub_url, '--output', 'temp_sitemap.gz'],
                capture_output=True
            )
            
            if result.returncode == 0:
                with gzip.open('temp_sitemap.gz', 'rb') as f:
                    xml_content = f.read().decode('utf-8', errors='ignore')
                
                print(f"    XML Snippet: {xml_content[:300]}")
                
                # Chercher les URLs produits
                # Pattern: /ex-en/markets/equ/fut/...-123456
                # ou /ex-en/markets/idx/.../...-123456
                urls = re.findall(r'<loc>(https://www\.eurex\.com/ex-en/markets/(?:equ/fut|idx)/[^<]+?-\d+)</loc>', xml_content)
                
                count = 0
                for url in urls:
                    if '/markets/equ/fut/' in url:
                        p_type = 'SSF'
                    else:
                        p_type = 'INDEX'
                    
                    slug_match = re.search(r'/([^/]+)-\d+$', url)
                    slug_name = slug_match.group(1).replace('-', ' ') if slug_match else ''
                    
                    products.append({
                        'url': url,
                        'slug_name': slug_name,
                        'product_type': p_type
                    })
                    count += 1
                
                print(f"{count} produits.")
            else:
                print("Erreur curl.")
                
        except Exception as e:
            print(f"Erreur: {e}")
            
    # Nettoyage
    if os.path.exists('temp_sitemap.gz'):
        os.remove('temp_sitemap.gz')
        
    return products

def _scrape_eurex_product_detail(session, product_url):
    """
    Scraper une page produit Eurex pour extraire les métadonnées riches.
    Utilise curl pour fiabilité.
    Retourne un dict avec: symbol, name, contract_size, currency, product_isin,
                           underlying_isin, contract_months
    """
    html = _fetch_url_content(product_url)
    if not html:
        return None
    
    try:
        # 1. Extraire le titre/symbole
        # Pattern validé: class="dbx-product-header__title">Name (SYMBOL)</h1>
        title_match = re.search(r'class="dbx-product-header__title">([^<]*?)\(([A-Z][A-Z0-9]{1,7})\)</h1>', html)
        if not title_match:
            # Fallback
            title_match = re.search(r'<title>\s*([^<]+?)\s*\(([A-Z][A-Z0-9]{1,7})\)', html, re.S)
        
        if not title_match:
            return None
            
        product_name = title_match.group(1).strip()
        future_symbol = title_match.group(2)
        
        # 2. Contract size
        contract_size = 1
        # Chercher "Contract size" puis digits
        cs_idx = html.find("Contract size")
        if cs_idx > 0:
            snippet = html[cs_idx:cs_idx+400]
            cs_match = re.search(r'Contract\s+size.*?(\d+)', snippet, re.S | re.I)
            if cs_match:
                contract_size = int(cs_match.group(1))
        
        # 3. Currency
        currency = 'EUR'
        # Chercher Currency puis code ISO standard
        curr_match = re.search(r'Currency.*?\b(EUR|USD|GBP|CHF|NOK|SEK|DKK|JPY)\b', html, re.S | re.I)
        if curr_match:
            currency = curr_match.group(1).upper()
        
        # 4. Product ISIN
        product_isin = ''
        pisin_match = re.search(r'Product\s+ISIN.*?([A-Z]{2}[A-Z0-9]{10})', html, re.S | re.I)
        if pisin_match:
            product_isin = pisin_match.group(1)
        
        # 5. Underlying ISIN
        underlying_isin = ''
        uisin_match = re.search(r'Underlying\s+ISIN.*?([A-Z]{2}[A-Z0-9]{10})', html, re.S | re.I)
        if uisin_match:
            underlying_isin = uisin_match.group(1)
        
        # 6. Contract months / Expirations
        # Pattern: dates "Last Trading Day" dans le calendrier
        expiration_dates = []
        ltd_pattern = re.findall(
            r'(\w{3})\s*\n?\s*(\d{2})\s*\n?.*?Last\s+Trading\s+Day',
            html, re.S | re.I
        )
        if not ltd_pattern:
             # Essayer pattern alternatif pour le calendrier
             ltd_pattern = re.findall(r'(\d{2})/(\d{2})/(\d{4})', html)
             ## Ce pattern est trop générique, restons sur le scraping sommaire ou extraction simple
        
        month_map = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 
                      'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                      'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
        current_year = datetime.now().year
        
        # Si regex calendrier échoue, on génère des expirations basiques
        # ... mais essayons de parser les dates trouvées
        for month_str, day in ltd_pattern:
            if month_str in month_map:
                # Deviner l'année est complexe sans contexte, on suppose année courante ou suivante
                # Pour l'instant on stocke juste MM/DD
                pass

        return {
            'symbol': future_symbol,
            'name': product_name,
            'contract_size': contract_size,
            'currency': currency,
            'product_isin': product_isin,
            'underlying_isin': underlying_isin,
            'expirations': [] # Complexe à extraire parfaitement sans JS, on laisse vide pour l'instant
        }
    except Exception as e:
        return None


def download_eurex_data():
    """
    Télécharger les données futures Eurex via Sitemap + Scraping détail.
    Utilise curl pour robustesse.
    Phase 1: Collecter les liens via Sitemap.
    Phase 2: Scraper chaque page produit pour les métadonnées.
    """
    print("\n=== TÉLÉCHARGEMENT EXHAUSTIF EUREX (via Sitemap + Curl) ===")
    
    db = UniverseDatabase()
    session = None
    
    # Désactiver tous les anciens assets Eurex avant mise à jour
    # Les assets trouvés seront réactivés automatiquement (active=1 dans l'upsert)
    print("  Désactivation des anciens assets SCRAPER_EUREX...")
    db.deactivate_all_from_source('SCRAPER_EUREX')
    
    # ── Phase 1: Collecter tous les liens produits via Sitemap ──
    print("\n--- Phase 1: Collecte des liens produits (Sitemap) ---")
    all_products = _get_product_urls_from_sitemap()
    
    # Filtrer doublons
    unique_products = []
    seen = set()
    for p in all_products:
        if p['url'] not in seen:
            unique_products.append(p)
            seen.add(p['url'])
    all_products = unique_products
    
    print(f"  Total liens trouvés: {len(all_products)}")
    print(f"  Dont SSF: {len([p for p in all_products if p['product_type'] == 'SSF'])}")
    print(f"  Dont Index: {len([p for p in all_products if p['product_type'] == 'INDEX'])}")
    
    if not all_products:
        print("Aucun produit trouvé. Vérifiez la connexion ou le sitemap.")
        return False

    # ── Phase 2: Scraper chaque page produit ──
    print(f"\n--- Phase 2: Scraping des {len(all_products)} pages produits ---")
    total_added = 0
    total_failed = 0
    
    # Trier pour grouper SSF et Index
    all_products.sort(key=lambda x: x['product_type'])
    
    # Cache ISIN -> ticker pour résolution underlying Eurex
    isin_ticker_cache_eurex = {}
    
    for i, product in enumerate(all_products):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"  Progression: {i + 1}/{len(all_products)} ({total_added} ajoutés)")
        
        detail = _scrape_eurex_product_detail(session, product['url'])
        
        if detail and detail.get('symbol'):
            underlying_isin_eurex = detail.get('underlying_isin', '')
            
            # Résoudre le ticker du sous-jacent via la cascade (Euronext → OpenFIGI)
            underlying_ticker, underlying_exchange_eurex = resolve_underlying_ticker(
                underlying_isin_eurex, isin_ticker_cache_eurex
            )
            
            # Fallback: dériver la place de cotation depuis le pays de l'ISIN
            if not underlying_exchange_eurex and len(underlying_isin_eurex) == 12:
                country = underlying_isin_eurex[:2]
                underlying_exchange_eurex = _ISIN_COUNTRY_TO_IBKR_EXCHANGE.get(country, '')
            
            # underlying_symbol = ticker résolu (jamais l'ISIN brut)
            underlying_symbol = underlying_ticker
            
            success = db.add_future_asset(
                symbol=detail['symbol'],
                future_symbol=detail['symbol'],
                name=detail['name'],
                underlying_symbol=underlying_symbol,
                exchange='DTB',
                currency=detail.get('currency', 'EUR'),
                source='SCRAPER_EUREX',
                derivatives_exchange='DTB',
                underlying_primary_exchange=underlying_exchange_eurex,
                contract_size=detail.get('contract_size', 1),
                multiplier=detail.get('contract_size', 1),
                expirations='',
                underlying_isin=underlying_isin_eurex,
                derivative_category=product['product_type']
            )
            
            if success:
                total_added += 1
                status = "✓"
                ticker_display = underlying_symbol or underlying_isin_eurex or '?'
                print(f"    {status} {detail['symbol']:8s} | {detail['name'][:30]:30s} | {detail.get('contract_size',1)} | {ticker_display} | {detail.get('currency')}", flush=True)
            else:
                total_failed += 1
                print(f"    ✗ DB Error: {detail['symbol']}", flush=True)

        else:
            total_failed += 1
            if (i + 1) % 50 == 0:
                 print(f"    ✗ Pas de données pour {product['url'].split('/')[-1]}")
        
        time.sleep(0.1) # Un peu plus rapide car curl lance un process à chaque fois

    print(f"\n=== RÉSUMÉ EUREX ===")
    print(f"Total produits scrapés: {len(all_products)}")
    print(f"Total ajoutés en DB: {total_added}")
    return True

def update_universe(categories=None):
    """
    Mettre à jour l'univers de futures depuis Euronext et Eurex.
    
    Args:
        categories: Liste des catégories à scraper (défaut: toutes)
    """
    print("Mise à jour de l'univers d'investissement (Mode Enricchi Direct)...")

    # 1. Téléchargement Euronext (Direct DB + Enrichment)
    print("\n=== EURONEXT ===")
    download_euronext_data(categories=categories)
    
    # 2. Téléchargement Eurex (Direct DB)
    # Note: Eurex n'a pas de filtrage par catégorie dans cette version
    print("\n=== EUREX ===")
    download_eurex_data()
    
    # Résumé final
    db = UniverseDatabase()
    all_futures = db.get_all_futures()
    print(f"\nTotal futures dans la base: {len(all_futures)}")
    with_underlying = sum(1 for f in all_futures if f['underlying_symbol'])
    with_expirations = sum(1 for f in all_futures if f['expirations'])
    print(f"  - Avec sous-jacent identifié: {with_underlying}")
    print(f"  - Avec expirations: {with_expirations}")

def validate_futures_contracts():
    """
    Valider les contrats futures en les comparant avec les données IBKR.
    """
    print("\n=== VALIDATION DES CONTRATS FUTURES ===")
    
    # Cette fonction pourra être améliorée pour appeler l'API IBKR
    # et valider que les symboles futures existent réellement
    print("  [INFO] Validation basique des symboles futures")
    
    db = UniverseDatabase()
    all_assets = db.get_active_assets()
    
    futures_symbols = [a['future_symbol'] for a in all_assets if a['future_symbol'] != a['symbol']]
    unique_futures = list(set(futures_symbols))
    
    print(f"  -> {len(unique_futures)} symboles futures uniques trouvés")
    print(f"  -> Exemples: {unique_futures[:10]}")
    
    # Vérification basique des symboles
    valid_futures = []
    for symbol in unique_futures:
        if len(symbol) <= 6 and symbol.isalnum():  # Vérification basique
            valid_futures.append(symbol)
    
    print(f"  -> {len(valid_futures)} symboles valides après vérification basique")
    
    return valid_futures

def main():
    """
    Fonction principale pour exécuter les mises à jour de l'univers.
    Options en ligne de commande :
      --type index   : Uniquement les futures sur indices
      --type ssf     : Uniquement les Single Stock Futures
      --type ssdf    : Uniquement les Dividend Stock Futures
      (sans option)  : Tous les types
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Mise à jour de l\'univers de futures Euronext/Eurex'
    )
    parser.add_argument(
        '--type', '-t',
        choices=['index', 'ssf', 'ssdf', 'all'],
        default='all',
        help='Type de futures à scraper: index (indices), ssf (stock futures), ssdf (dividend stock futures), all (tous)'
    )
    
    args = parser.parse_args()
    
    # Mapper les arguments vers les catégories Euronext
    category_map = {
        'index': ['index-futures'],
        'ssf': ['stock-futures'],
        'ssdf': ['dividend-stock-futures'],
        'all': ['stock-futures', 'index-futures', 'dividend-stock-futures']
    }
    
    selected_categories = category_map.get(args.type, category_map['all'])
    
    print("=== MISE À JOUR DE L'UNIVERS FUTURES ===")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Types sélectionnés: {', '.join(selected_categories)}")
    
    # 1. Mettre à jour l'univers de base
    update_universe(categories=selected_categories)
    
    # 2. Valider les contrats futures
    valid_futures = validate_futures_contracts()
    
    print(f"\n=== VALIDATION TERMINÉE ===")
    print(f"{len(valid_futures)} contrats futures validés")
    print("L'univers est prêt pour le trading Cash & Carry")

if __name__ == "__main__":
    main()

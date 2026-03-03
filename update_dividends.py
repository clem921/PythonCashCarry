#!/usr/bin/env python3
"""
Script pour mettre a jour les informations de dividendes dans la base de donnees.

Sources disponibles:
  - yfinance (defaut) : historique dividendes via Yahoo Finance
  - ibkr : donnees live depuis IBKR TWS (generic tick 456) — requiert TWS connecte

Usage:
    python update_dividends.py                          # yfinance, tous les actifs
    python update_dividends.py --source ibkr            # IBKR, tous les actifs (TWS requis)
    python update_dividends.py --source ibkr -s AS6     # IBKR, un symbole
    python update_dividends.py --source ibkr -c EUR     # IBKR, actifs EUR uniquement
    python update_dividends.py --symbol MC              # yfinance, un symbole
    python update_dividends.py --currency EUR            # yfinance, actifs EUR
    python update_dividends.py --show                   # Afficher les dividendes a venir
"""

import argparse
import sys
import pandas as pd
import time
from datetime import datetime
from threading import Thread, Event
from universe_manager import UniverseDatabase
import config

# Mapping des suffixes yfinance par exchange
# Utilise primary_exchange (config) ou derivatives_exchange (base SQLite)
YFINANCE_SUFFIXES = {

    # Derivatives exchanges (depuis universe.db - codes Euronext)
    'DPAR': '.PA',   # Euronext Paris Derivatives
    'DAMS': '.AS',   # Euronext Amsterdam Derivatives
    'DBRU': '.BR',   # Euronext Brussels Derivatives
    'DMIL': '.MI',   # Euronext Milan Derivatives (Italie)
    'DOSL': '.OL',   # Euronext Oslo Derivatives (Norvège)
    'DLIS': '.LS',   # Euronext Lisbon Derivatives (Portugal)
    'DTB': '.DE',    # Eurex Germany
    'FTA': '.PA',    # Euronext Derivatives (unifié, fallback vers Paris)
    'LSSF': '.AS',   # Euronext SSF (fallback vers Amsterdam)
    # Primary exchanges (IBKR codes)
    'SBF': '.PA',    # Paris
    'AEB': '.AS',    # Amsterdam
    'BVB': '.BR',    # Brussels
    'EBR': '.BR',    # Brussels (variante)
    'IBIS': '.DE',   # Xetra / Francfort
    'LSE': '.L',     # London
    'BVME': '.MI',   # Borsa Italiana (Milan)
    'EBS': '.SW',    # SIX Swiss Exchange
    'BM': '.MC',     # Bolsa de Madrid
    'OSE': '.OL',    # Oslo
    'HEX': '.HE',    # Helsinki (Nasdaq Nordic)
    'CSE': '.CO',    # Copenhagen (Nasdaq Nordic)
    'SFB': '.ST',    # Stockholm (Nasdaq Nordic)
    'VSE': '.VI',    # Vienna
    'ENXL': '.LS',   # Euronext Lisbon
    'ISE': '.IR',    # Irish Stock Exchange (Dublin)
    'US': '',        # USA (pas de suffixe)
    '': '',
}



def get_yfinance_ticker(symbol, primary_exchange='', derivatives_exchange='', underlying_exchange=''):
    """Construire le ticker yfinance à partir du symbole et des exchanges
    
    Args:
        symbol: Symbole de l'actif (ex: 'MC', 'ASML', 'MSFT')
        primary_exchange: Code exchange primaire (ex: 'SBF', 'AEB', 'IBIS', 'US')
        derivatives_exchange: Code exchange dérivés (ex: 'DPAR', 'DAMS', 'DTB')
        underlying_exchange: Exchange du sous-jacent (ex: 'LSE')
    
    Returns:
        Ticker yfinance (ex: 'MC.PA', 'ASML.AS', 'MSFT', 'ABF.L')
    """
    # Si le symbole ressemble déjà à un ticker avec suffixe, on le garde
    if '.' in symbol and any(symbol.endswith(s) for s in YFINANCE_SUFFIXES.values() if s):
        return symbol
        
    # Cas particulier: Actions US listées sur Euronext ou Eurex
    # Si primary_exchange ou underlying_exchange est US, pas de suffixe
    if primary_exchange == 'US' or underlying_exchange == 'US':
        return symbol

    # Priorité: 
    # 1. underlying_exchange (pour les futures/dérivés)
    # 2. primary_exchange
    # 3. derivatives_exchange
    
    potential_exchanges = [underlying_exchange, primary_exchange, derivatives_exchange]
    suffix = ''
    
    for exch in potential_exchanges:
        if exch and exch in YFINANCE_SUFFIXES:
            suffix = YFINANCE_SUFFIXES[exch]
            break
    
    # Tickers LSE: IBKR utilise '.' (AV., BP., BT.A) et OpenFIGI '/' (AV/, BP/, BT/A)
    # yfinance attend le symbole nu ou avec '-' pour les classes (AV, BP, BT-A)
    if '/' in symbol:
        symbol = symbol.rstrip('/').replace('/', '-')
    elif suffix == '.L' and symbol.endswith('.'):
        symbol = symbol[:-1]
    elif suffix == '.L' and '.' in symbol:
        symbol = symbol.replace('.', '-')
            
    return f"{symbol}{suffix}"


def fetch_dividend_info(symbol, primary_exchange='', derivatives_exchange='', underlying_exchange=''):
    """Récupérer les informations de dividende depuis yfinance
    
    Args:
        symbol: Symbole de l'actif
        primary_exchange: Code exchange primaire (ex: 'SBF', 'AEB')
        derivatives_exchange: Code exchange dérivés (ex: 'DPAR', 'DAMS', 'DTB')
        underlying_exchange: Exchange du sous-jacent (ex: 'LSE')
    
    Returns:
        dict avec ex_dividend_date, payment_date, dividend_amount ou None si erreur
    """
    try:
        import yfinance as yf
    except ImportError:
        print("ERREUR: yfinance n'est pas installé. Exécutez: pip install yfinance")
        return None
    
    ticker_str = get_yfinance_ticker(symbol, primary_exchange, derivatives_exchange, underlying_exchange)
    
    try:
        ticker = yf.Ticker(ticker_str)
        
        # Vérifier si le ticker est valide/accessible
        # yfinance ne lève pas toujours une exception sur 404, mais l'historique sera vide
        try:
            # Utiliser fast_info ou history pour valider l'existence
            if not ticker.history(period="1d").empty:
                is_available = True
            else:
                # Fallback: certains actifs n'ont pas d'historique de prix mais ont des métadonnées
                # On check si on peut récupérer les dividendes au moins
                is_available = False
        except Exception:
            is_available = False

        # Récupérer les dividendes historiques
        dividends = ticker.dividends
        
        if dividends is None or dividends.empty:
            # Si pas de dividendes ET pas d'historique, on considère non disponible
            if not is_available:
                return None
                
            # Pas de dividende trouvé mais ticker valide
            return {
                'ex_dividend_date': '',
                'payment_date': '',
                'dividend_amount': 0
            }
        
        # ... (reste de la logique inchangé pour le calcul)
        
        # Le dernier dividende (le plus récent)
        last_dividend = dividends.iloc[-1]
        last_date = dividends.index[-1]
        
        # yfinance donne la date ex-dividende
        ex_dividend_date = last_date.strftime('%Y-%m-%d')
        dividend_amount = float(last_dividend)
        
        # Essayer de récupérer la date de paiement depuis le calendrier
        payment_date = ''
        try:
            calendar = ticker.calendar
            if calendar is not None and not calendar.empty:
                # Dans les versions récentes de yfinance, calendar peut être un DataFrame
                # avec les lignes 'Dividend Date' ou 'Ex-Dividend Date'
                if isinstance(calendar, pd.DataFrame):
                    if 'Dividend Date' in calendar.index:
                        pd_date = calendar.loc['Dividend Date'].iloc[0]
                        if pd_date and not pd.isna(pd_date):
                            payment_date = pd.to_datetime(pd_date).strftime('%Y-%m-%d')
                    elif 'Dividends' in calendar.index: # Fallback
                        pd_date = calendar.loc['Dividends'].iloc[0]
                        if pd_date and not pd.isna(pd_date):
                            payment_date = pd.to_datetime(pd_date).strftime('%Y-%m-%d')
        except Exception:
            pass
        
        # Si le dividende est dans le futur (date ex > aujourd'hui), c'est le prochain
        # Sinon, c'est un historique
        today = datetime.now().strftime('%Y-%m-%d')
        if ex_dividend_date < today:
            # Dividende passé, pas de prochain dividende connu
            # On garde les infos comme référence (montant du dernier dividende)
            pass
        
        return {
            'ex_dividend_date': ex_dividend_date,
            'payment_date': payment_date,
            'dividend_amount': dividend_amount
        }
        
    except Exception as e:
        print(f"  Erreur yfinance pour {ticker_str}: {e}")
        return None


def update_all_dividends(db, currency_filter=None, category_filter=None):
    """Mettre à jour les dividendes pour tous les actifs actifs
    
    Args:
        db: Instance UniverseDatabase
        currency_filter: Filtrer par devise (optionnel)
        category_filter: Filtrer par catégorie (optionnel)
    """
    assets = db.get_active_assets(region_currency=currency_filter, category=category_filter)
    
    print(f"Mise à jour des dividendes pour {len(assets)} actifs...")
    print("-" * 60)
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    
    
    try:
        for asset in assets:
            symbol = asset['symbol']
            underlying_symbol = asset.get('underlying_symbol', '')
            primary_exchange = asset.get('primary_exchange', '')
            derivatives_exchange = asset.get('derivatives_exchange', '')
            underlying_primary_exchange = asset.get('underlying_primary_exchange', '')
            exchange = asset.get('exchange', 'SMART')
            currency = asset['currency']
            derivative_category = asset.get('derivative_category', '')
            
            # Ignorer les actifs sans underlying_symbol (pas de ticker sous-jacent pour dividendes)
            # SAUF pour les dividend futures où on utilise le symbol en direct
            is_dividend_future = 'dividend' in derivative_category.lower()
            
            
            if not underlying_symbol and not is_dividend_future:
                print(f"\n[{symbol}] IGNORÉ - pas de underlying_symbol et n'est pas un dividend future")
                skipped_count += 1
                continue
            
            # Déterminer le ticker à utiliser pour yfinance
            # Pour les dividend futures, on utilise le symbol, sinon l'underlying
            ticker_to_fetch = symbol if is_dividend_future else underlying_symbol
            
            print(f"\n[{symbol}] ticker_to_fetch={ticker_to_fetch} (category={derivative_category}, primary={primary_exchange}, deriv={derivatives_exchange}, underlying_exch={underlying_primary_exchange}, {currency})")
            
            # Récupérer les infos de dividende
            div_info = fetch_dividend_info(ticker_to_fetch, primary_exchange, derivatives_exchange, underlying_primary_exchange)
            
            if div_info is None:
                print(f"  ERREUR technique pour {ticker_to_fetch} (yfinance non disponible)")
                db.update_dividend(symbol=symbol, exchange=exchange, currency=currency, yfinance_available=0)
                error_count += 1
                continue
            
            # Pause pour éviter le rate limiting (yfinance)
            time.sleep(1.0) # Un peu plus long pour être sûr
            
            # Mettre à jour la base de données
            if div_info['dividend_amount'] > 0:
                success = db.update_dividend(
                    symbol=symbol,
                    ex_dividend_date=div_info['ex_dividend_date'],
                    payment_date=div_info['payment_date'],
                    dividend_amount=div_info['dividend_amount'],
                    exchange=exchange,
                    currency=currency,
                    yfinance_available=1
                )
                if success:
                    success_count += 1
            else:
                print(f"  Aucun dividende trouvé")
                # Effacer les anciennes données si existantes, mais marquer comme disponible
                db.update_dividend(symbol=symbol, exchange=exchange, currency=currency, yfinance_available=1)
    except KeyboardInterrupt:
        print("\n\nINTERRUPTION par l'utilisateur. Arrêt de la mise à jour...")
    
    print("\n" + "=" * 60)
    print(f"Terminé: {success_count} réussites, {error_count} erreurs, {skipped_count} ignorés (sans underlying)")


def update_single_dividend(db, symbol, currency='EUR'):
    """Mettre à jour le dividende pour un seul symbole
    
    Args:
        db: Instance UniverseDatabase
        symbol: Symbole de l'actif
        currency: Devise
    """
    # Récupérer les infos de l'actif
    asset = db.get_asset_by_symbol(symbol)
    
    if asset is None:
        print(f"ERREUR: Actif {symbol} non trouvé dans la base")
        return
    
    primary_exchange = asset.get('primary_exchange', '')
    derivatives_exchange = asset.get('derivatives_exchange', '')
    underlying_primary_exchange = asset.get('underlying_primary_exchange', '')
    currency = asset.get('currency', currency)
    underlying_symbol = asset.get('underlying_symbol', symbol) or symbol
    exchange = asset.get('exchange', 'SMART')
    derivative_category = asset.get('derivative_category', '')
    
    is_dividend_future = 'dividend' in derivative_category.lower()
    ticker_to_fetch = symbol if is_dividend_future else (underlying_symbol or symbol)
    
    print(f"Récupération du dividende pour {symbol} (fetch={ticker_to_fetch}, category={derivative_category}, primary={primary_exchange}, deriv={derivatives_exchange}, underlying_exch={underlying_primary_exchange})...")
    
    div_info = fetch_dividend_info(ticker_to_fetch, primary_exchange, derivatives_exchange, underlying_primary_exchange)
    
    if div_info is None:
        print(f"ERREUR: Impossible de récupérer les informations pour {ticker_to_fetch}")
        db.update_dividend(symbol=symbol, exchange=exchange, currency=currency, yfinance_available=0)
        return
    
    if div_info['dividend_amount'] > 0:
        db.update_dividend(
            symbol=symbol,
            ex_dividend_date=div_info['ex_dividend_date'],
            payment_date=div_info['payment_date'],
            dividend_amount=div_info['dividend_amount'],
            exchange=exchange,
            currency=currency,
            yfinance_available=1
        )
    else:
        print(f"Aucun dividende trouvé pour {symbol}")
        db.update_dividend(symbol=symbol, exchange=exchange, currency=currency, yfinance_available=1)


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE IBKR : Recuperation des dividendes via l'API native IBKR (generic tick 456)
# ═══════════════════════════════════════════════════════════════════════════════

class IBDividendApp:
    """Client IBKR leger dedie a la recuperation des dividendes.
    
    Utilise reqMktData avec genericTickList="456" qui retourne les infos dividende
    via le callback tickString (tickType 59).
    Format: "past12m,nextDate,nextAmount,nextAnnual" (separateur virgule ou point-virgule)
    """

    def __init__(self):
        # Import IBKR API
        sys.path.append(r'C:/TWS API/source/pythonclient')
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        
        # Creer la classe dynamiquement pour eviter l'import global
        parent = self
        
        class _App(EWrapper, EClient):
            def __init__(self):
                EClient.__init__(self, self)
                self.connected = False
                self.dividend_data = {}   # reqId -> dividend string
                self.data_events = {}     # reqId -> Event
                self._next_req_id = 20000

            def get_next_req_id(self):
                req_id = self._next_req_id
                self._next_req_id += 1
                return req_id

            def nextValidId(self, orderId):
                self.connected = True
                print(f"Connecte a IBKR TWS (orderId={orderId})")

            def tickString(self, reqId, tickType, value):
                """Callback pour les donnees textuelles — tickType 59 = IB Dividends"""
                super().tickString(reqId, tickType, value)
                if tickType == 59 and value:
                    self.dividend_data[reqId] = value
                    if reqId in self.data_events:
                        self.data_events[reqId].set()

            def tickPrice(self, reqId, tickType, price, attrib):
                """Ignorer les ticks de prix (on ne veut que les dividendes)"""
                pass

            def tickSize(self, reqId, tickType, size):
                pass

            def tickGeneric(self, reqId, tickType, value):
                pass

            def tickSnapshotEnd(self, reqId):
                """Fin du snapshot — liberer l'event meme si pas de dividende recu"""
                if reqId in self.data_events:
                    self.data_events[reqId].set()

            def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
                silent_codes = [2104, 2106, 2158, 10167]
                if errorCode in silent_codes:
                    pass
                elif errorCode == 200:
                    # Pas de definition de titre — pas bloquant
                    if reqId in self.data_events:
                        self.data_events[reqId].set()
                else:
                    if errorCode not in [10090]:  # market data farm connecting
                        print(f"  [IBKR] Erreur reqId={reqId} code={errorCode}: {errorString}")
                    if reqId in self.data_events:
                        self.data_events[reqId].set()

        self.app = _App()

    def connect(self):
        """Se connecter a IBKR TWS"""
        host = config.IBKR_CONFIG['host']
        port = config.IBKR_CONFIG['port']
        client_id = config.IBKR_CONFIG['client_id'] + 10  # Eviter conflit avec main.py

        self.app.connect(host, port, clientId=client_id)
        thread = Thread(target=self.app.run, daemon=True)
        thread.start()

        # Attendre la connexion
        for _ in range(30):
            if self.app.connected:
                # Configurer le type de donnees de marche
                self.app.reqMarketDataType(config.MARKET_DATA_TYPE)
                return True
            time.sleep(0.2)
        
        print("ERREUR: Impossible de se connecter a IBKR TWS")
        return False

    def disconnect(self):
        self.app.disconnect()
        print("Deconnecte de IBKR TWS")

    def fetch_dividend(self, symbol, primary_exchange='', currency='EUR'):
        """Recuperer les infos dividende pour un symbole via IBKR.
        
        Strategie de resolution :
        1. Essayer avec primaryExchange (si fourni)
        2. Fallback sans primaryExchange (juste SMART + currency)
        3. Pour les symboles nordiques composites (NOVOB, NSISB...),
           essayer avec un espace avant le suffixe de classe (NOVO B, NSIS B)
        
        Args:
            symbol: Symbole du sous-jacent (ex: 'ASML', 'ULVR')
            primary_exchange: Exchange primaire (ex: 'AEB', 'LSE')
            currency: Devise native du sous-jacent
        
        Returns:
            dict avec ibkr_div_next_date, ibkr_div_next_amount, ibkr_div_past_12m, 
            ibkr_div_annual, ou None si echec
        """
        # Construire la liste de tentatives : (symbole, primaryExchange)
        attempts = []
        if primary_exchange:
            attempts.append((symbol, primary_exchange))
        attempts.append((symbol, ''))  # Fallback sans primaryExchange
        
        # Pour les symboles nordiques composites (suffixe A/B),
        # ajouter une tentative avec espace : NOVOB -> NOVO B
        nordic_exchanges = {'SFB', 'CSE', 'HEX', 'OSE'}
        if primary_exchange in nordic_exchanges and len(symbol) > 1 and symbol[-1] in 'AB':
            spaced = symbol[:-1] + ' ' + symbol[-1]
            attempts.append((spaced, primary_exchange))
            attempts.append((spaced, ''))
        
        from ibapi.contract import Contract

        for try_symbol, try_exchange in attempts:
            contract = Contract()
            contract.symbol = try_symbol
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = currency
            if try_exchange:
                contract.primaryExchange = try_exchange

            req_id = self.app.get_next_req_id()
            self.app.data_events[req_id] = Event()

            self.app.reqMktData(req_id, contract, "456", False, False, [])
            self.app.data_events[req_id].wait(timeout=6)
            self.app.cancelMktData(req_id)

            raw = self.app.dividend_data.get(req_id)
            if raw:
                return self._parse_dividend_string(raw, symbol)

        return None

    def _parse_dividend_string(self, raw, symbol=''):
        """Parser la chaine de dividende IBKR (generic tick 456, tickType 59).
        
        Format IBKR documente:
            "past12m,nextDate,nextAmount,nextDate2,nextAmount2,...,annualAmount"
        Le separateur peut etre ',' ou ';'.
        Les champs sont: total 12 mois, puis paires (date, montant) pour chaque
        dividende futur, et en dernier le dividende annuel estime.
        """
        # Determiner le separateur
        if ';' in raw:
            parts = raw.split(';')
        else:
            parts = raw.split(',')

        result = {
            'ibkr_div_past_12m': 0,
            'ibkr_div_next_date': '',
            'ibkr_div_next_amount': 0,
            'ibkr_div_annual': 0,
        }

        try:
            # Classifier chaque element : nombre ou date (YYYYMMDD)
            numbers = []
            dates = []
            for p in parts:
                p = p.strip()
                if not p:
                    continue
                if len(p) == 8 and p.isdigit() and p[:2] in ('19', '20'):
                    # C'est une date YYYYMMDD
                    dates.append(f"{p[:4]}-{p[4:6]}-{p[6:8]}")
                else:
                    try:
                        numbers.append(float(p))
                    except ValueError:
                        pass

            # Interpretation :
            # - Premier nombre = total dividendes 12 derniers mois
            # - Dernier nombre = dividende annuel estime
            # - Nombres intermediaires = montants des prochains dividendes
            # - Dates = dates ex-dividende correspondantes
            if numbers:
                result['ibkr_div_past_12m'] = numbers[0]
            if len(numbers) >= 2:
                result['ibkr_div_annual'] = numbers[-1]
            if len(numbers) >= 3:
                # Les nombres entre le premier et le dernier sont les montants
                result['ibkr_div_next_amount'] = numbers[1]
            if dates:
                result['ibkr_div_next_date'] = dates[0]  # Premiere date = prochain ex-div

        except Exception as e:
            print(f"  [IBKR] Erreur parsing dividende '{raw}': {e}")

        return result


# Mapping exchange derives Euronext -> exchange actions IBKR (pour fallback)
# Utilise quand underlying_primary_exchange est vide
_DERIV_TO_STOCK_EXCHANGE = {
    'DPAR': 'SBF',    # Euronext Paris Derivatives -> Paris
    'DAMS': 'AEB',    # Euronext Amsterdam Derivatives -> Amsterdam
    'DBRU': 'EBR',    # Euronext Brussels Derivatives -> Brussels
    'DMIL': 'BVME',   # Euronext Milan Derivatives -> Milan
    'DOSL': 'OSE',    # Euronext Oslo Derivatives -> Oslo
    'DLIS': 'ENXL',   # Euronext Lisbon Derivatives -> Lisbon
}


def update_dividends_ibkr(db, currency_filter=None, category_filter=None, symbol_filter=None):
    """Mettre a jour les dividendes via l'API IBKR (generic tick 456).
    
    Se connecte a TWS, parcourt les actifs de la base et recupere les infos
    de dividende pour chaque sous-jacent.
    
    Args:
        db: Instance UniverseDatabase
        currency_filter: Filtrer par devise (optionnel)
        category_filter: Filtrer par categorie (optionnel)
        symbol_filter: Mettre a jour un seul symbole (optionnel)
    """
    ibkr = IBDividendApp()
    if not ibkr.connect():
        return

    if symbol_filter:
        asset = db.get_asset_by_symbol(symbol_filter)
        if not asset:
            print(f"ERREUR: Actif {symbol_filter} non trouve dans la base")
            ibkr.disconnect()
            return
        assets = [asset]
    else:
        assets = db.get_active_assets(region_currency=currency_filter, category=category_filter)

    print(f"\nMise a jour des dividendes via IBKR pour {len(assets)} actifs...")
    print("-" * 60)

    success_count = 0
    error_count = 0
    skipped_count = 0
    # Deduplication par sous-jacent pour eviter les requetes redondantes
    # (ex: AS6, AS7 ont le meme sous-jacent ASML)
    seen_underlyings = {}  # underlying_symbol -> resultat

    try:
        for asset in assets:
            symbol = asset['symbol']
            underlying_symbol = asset.get('underlying_symbol', '')
            primary_exchange = asset.get('primary_exchange', '')
            underlying_primary_exchange = asset.get('underlying_primary_exchange', '')
            derivatives_exchange = asset.get('derivatives_exchange', '')
            exchange = asset.get('exchange', 'SMART')
            currency = asset['currency']
            derivative_category = asset.get('derivative_category', '')

            # Le ticker a interroger est le sous-jacent
            ticker = underlying_symbol or symbol
            if not ticker:
                skipped_count += 1
                continue

            # Ignorer les dividend-stock-futures dont le underlying_symbol est
            # un ISIN synthetique (SSDF...) et non un vrai symbole action
            if ticker.startswith('SSDF') or ticker.startswith('QS00'):
                skipped_count += 1
                continue

            # Determiner l'exchange du sous-jacent :
            # 1. underlying_primary_exchange (explicite)
            # 2. Deriver depuis derivatives_exchange (DPAR->SBF, DAMS->AEB...)
            # NB: primary_exchange pour un FUT = derivatives_exchange, pas l'exchange actions
            und_exchange = underlying_primary_exchange
            if not und_exchange and derivatives_exchange:
                und_exchange = _DERIV_TO_STOCK_EXCHANGE.get(derivatives_exchange, '')
            native_currency = config.EXCHANGE_CURRENCY.get(und_exchange, currency)

            # Deduplication : reutiliser le resultat si deja recupere
            dedup_key = (ticker, und_exchange, native_currency)
            if dedup_key in seen_underlyings:
                div_info = seen_underlyings[dedup_key]
                print(f"[{symbol}] Reutilisation du resultat pour {ticker} (deja recupere)")
            else:
                print(f"[{symbol}] Requete IBKR pour {ticker} (exchange={und_exchange}, devise={native_currency})...", end=" ")
                div_info = ibkr.fetch_dividend(ticker, und_exchange, native_currency)
                seen_underlyings[dedup_key] = div_info

                if div_info is None:
                    print("Pas de donnee dividende")
                    error_count += 1
                    # Pause entre les requetes
                    time.sleep(0.3)
                    continue
                else:
                    next_d = div_info['ibkr_div_next_date'] or '-'
                    next_a = div_info['ibkr_div_next_amount']
                    past = div_info['ibkr_div_past_12m']
                    annual = div_info['ibkr_div_annual']
                    print(f"OK next={next_d} amt={next_a:.4f} past12m={past:.4f} annual={annual:.4f}")

            if div_info is None:
                error_count += 1
                continue

            # Sauvegarder en base
            success = db.update_dividend_ibkr(
                symbol=symbol,
                exchange=exchange,
                currency=currency,
                ibkr_div_next_date=div_info['ibkr_div_next_date'],
                ibkr_div_next_amount=div_info['ibkr_div_next_amount'],
                ibkr_div_past_12m=div_info['ibkr_div_past_12m'],
                ibkr_div_annual=div_info['ibkr_div_annual'],
            )
            if success:
                success_count += 1
            else:
                error_count += 1

            # Pause entre les requetes pour eviter le throttling IBKR
            time.sleep(0.3)

    except KeyboardInterrupt:
        print("\n\nINTERRUPTION par l'utilisateur. Arret de la mise a jour...")

    ibkr.disconnect()

    print("\n" + "=" * 60)
    unique = len(seen_underlyings)
    print(f"Termine: {success_count} MAJ, {error_count} erreurs, {skipped_count} ignores")
    print(f"  {unique} sous-jacent(s) unique(s) interroge(s) pour {len(assets)} actifs")


def show_upcoming_dividends(db, currency_filter=None):
    """Afficher les dividendes à venir
    
    Args:
        db: Instance UniverseDatabase
        currency_filter: Filtrer par devise (optionnel)
    """
    dividends = db.get_upcoming_dividends(days_ahead=180, region_currency=currency_filter)
    
    if not dividends:
        print("Aucun dividende à venir dans les 180 jours")
        return
    
    print("\n" + "=" * 80)
    print("DIVIDENDES À VENIR (180 jours)")
    print("=" * 80)
    print(f"{'Symbole':<10} {'Ex-Date':<12} {'Paiement':<12} {'Montant':<10} {'Devise':<6}")
    print("-" * 80)
    
    for div in dividends:
        print(f"{div['symbol']:<10} {div['ex_dividend_date']:<12} {div['payment_date']:<12} {div['dividend_amount']:<10.4f} {div['currency']:<6}")


def main():
    parser = argparse.ArgumentParser(
        description='Mettre à jour les dividendes dans la base de données'
    )
    parser.add_argument(
        '--symbol', '-s',
        type=str,
        help='Mettre à jour un symbole spécifique uniquement'
    )
    parser.add_argument(
        '--currency', '-c',
        type=str,
        help='Filtrer par devise (ex: EUR, USD)'
    )
    parser.add_argument(
        '--type', '-t',
        type=str,
        help='Filtrer par type (ex: stock-futures, index-futures, dividend-stock-futures)'
    )
    parser.add_argument(
        '--show', '-S',
        action='store_true',
        help='Afficher uniquement les dividendes a venir (pas de mise a jour)'
    )
    parser.add_argument(
        '--source',
        choices=['yfinance', 'ibkr'],
        default='yfinance',
        help='Source des donnees: yfinance (defaut) ou ibkr (requiert TWS connecte)'
    )
    
    args = parser.parse_args()
    
    db = UniverseDatabase()
    
    if args.show:
        show_upcoming_dividends(db, args.currency)
    elif args.source == 'ibkr':
        update_dividends_ibkr(db,
                              currency_filter=args.currency,
                              category_filter=args.type,
                              symbol_filter=args.symbol)
        print("\n")
        show_upcoming_dividends(db, args.currency)
    elif args.symbol:
        update_single_dividend(db, args.symbol, args.currency or 'EUR')
    else:
        update_all_dividends(db, args.currency, args.type)
        print("\n")
        show_upcoming_dividends(db, args.currency)


if __name__ == "__main__":
    main()
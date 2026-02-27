#!/usr/bin/env python3
"""
Programme de trading Cash and Carry pour IBKR
Stratégie: Acheter un actif et vendre un contrat à terme pour profiter de l'écart de prix
Utilise l'API native IBKR au lieu de ib_insync
"""

import sys
import time
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from threading import Thread, Event

# Ajouter le chemin de l'API IBKR native
sys.path.append(r'C:/TWS API/source/pythonclient')

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import TickerId
from ibapi.scanner import ScannerSubscription

import config
from universe_manager import UniverseDatabase

# Configuration
IBKR_HOST = config.IBKR_CONFIG['host']
IBKR_PORT = config.IBKR_CONFIG['port']
IBKR_CLIENT_ID = config.IBKR_CONFIG['client_id']

# Configuration pour les différents modes
USE_REAL_TIME_DATA = not config.IBKR_CONFIG['paper_trading']
ACCOUNT_ID = config.IBKR_CONFIG['account_id']
DATA_MODE = "REALTIME" if USE_REAL_TIME_DATA else "DELAYED"

TWS_PATH = config.IBKR_CONFIG['tws_path']

# Configuration actuelle des marchés
CURRENT_MARKET = 'EUROZONE'

# Base de données SQLite
DB_NAME = config.DB_CONFIG['name']

class IBapi(EWrapper, EClient):
    """Classe wrapper pour l'API native IBKR avec gestion des callbacks"""

    def __init__(self):
        EClient.__init__(self, self)
        self.account_values = {}
        self.positions = []
        self.market_data = {}
        self.contract_details = {}
        self.contract_details_end_events = {}  # Events pour synchroniser la réception des détails
        self.scanner_data = {}
        self.next_order_id = None
        self.connected = False
        self.data_ready = False
        self._next_req_id = 10000  # Compteur monotone pour éviter les collisions de reqId
        self.whatif_results = {}  # orderId -> OrderState (résultats what-if)

    def get_next_req_id(self):
        """Retourner un reqId unique et incrémental (thread-safe pour usage simple)"""
        req_id = self._next_req_id
        self._next_req_id += 1
        return req_id

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_order_id = orderId
        self.connected = True
        print(f"Next valid order ID: {orderId}")
        print("Connected to IBKR TWS - Connexion établie via nextValidId")

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        self.account_values[key] = {'value': val, 'currency': currency, 'account': accountName}

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)
        self.positions.append({
            'account': account,
            'contract': contract,
            'position': position,
            'avgCost': avgCost
        })

    def tickPrice(self, reqId: TickerId, tickType: int, price: float, attrib):
        super().tickPrice(reqId, tickType, price, attrib)
        if reqId not in self.market_data:
            self.market_data[reqId] = {}
        self.market_data[reqId][tickType] = price

    def contractDetails(self, reqId: int, contractDetails):
        super().contractDetails(reqId, contractDetails)
        if reqId not in self.contract_details:
            self.contract_details[reqId] = []
        self.contract_details[reqId].append(contractDetails)

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        # Signaler que la réception est terminée pour ce reqId
        if reqId in self.contract_details_end_events:
            self.contract_details_end_events[reqId].set()

    def scannerData(self, reqId: int, rank: int, contractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark, projection, legsStr)
        if reqId not in self.scanner_data:
            self.scanner_data[reqId] = []
        self.scanner_data[reqId].append(contractDetails)
        print(f"Scanner Data: {rank} - {contractDetails.contract.symbol}")
    
    def scannerDataEnd(self, reqId: int):
        super().scannerDataEnd(reqId)
        print(f"Scanner Data End: {reqId}")

    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson: str = ""):
        # Messages purement informatifs (connexion data farm ok, etc.)
        silent_codes = [2104, 2106, 2158, 10167]
        # Messages importants pour le diagnostic mais pas bloquants
        warning_codes = [200, 1007]  # 200=ambiguous, 1007=no security definition found
        
        if errorCode in silent_codes:
            pass  # Ignorer silencieusement
        elif errorCode in warning_codes:
            print(f"  [WARNING] reqId={reqId} code={errorCode}: {errorString}")
            # Libérer l'event si en attente (la requête ne retournera plus rien)
            if reqId in self.contract_details_end_events:
                self.contract_details_end_events[reqId].set()
        else:
            super().error(reqId, errorCode, errorString, advancedOrderRejectJson)
            print(f"Error {reqId} {errorCode}: {errorString}")
            # Libérer l'event en cas d'erreur bloquante aussi
            if reqId in self.contract_details_end_events:
                self.contract_details_end_events[reqId].set()

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print(f"Order Status - Id: {orderId}, Status: {status}, Filled: {filled}, Remaining: {remaining}, Price: {avgFillPrice}")

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState):
        super().openOrder(orderId, contract, order, orderState)
        if order.whatIf:
            # Stocker le résultat what-if (commission estimée, marge, etc.)
            self.whatif_results[orderId] = orderState
        else:
            print(f"Open Order - Id: {orderId}, Symbol: {contract.symbol}, Action: {order.action}, Type: {order.orderType}")

    def connectionClosed(self):
        super().connectionClosed()
        self.connected = False
        print("Connection closed")

    def get_funding_rate(self, currency='EUR'):
        """Récupérer le taux de financement depuis les données du compte IBKR.
        Cherche les tags de taux d'intérêt dans les account values."""
        # Taux par défaut depuis la config
        default_rate = config.COST_CONFIG['funding_rate']
        
        # IBKR fournit parfois le taux d'intérêt courant via ces tags
        for tag in ['InterestRate', 'FullAvailableFunds']:
            if tag in self.account_values:
                try:
                    val = float(self.account_values[tag]['value'])
                    if 0 < val < 0.20:  # Sanity check (entre 0% et 20%)
                        return val
                except (ValueError, TypeError):
                    pass
        
        return default_rate

class CashCarryTrader:
    def __init__(self):
        self.ib = IBapi()
        self.connected = False
        self.positions = {}
        self.init_database()

        # Démarrer le thread pour l'API IBKR
        self.ib_thread = Thread(target=self.run_ib_api, daemon=True)
        self.ib_thread.start()

        # Attendre que la connexion soit établie
        time.sleep(2)

    def run_ib_api(self):
        """Méthode pour exécuter l'API IBKR dans un thread séparé"""
        self.ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID)
        self.ib.run()

    def init_database(self):
        """Initialiser la base de données SQLite"""
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_time TIMESTAMP NOT NULL,
                    future_symbol TEXT,
                    future_expiry TEXT,
                    status TEXT DEFAULT 'OPEN',
                    exit_price REAL,
                    exit_time TIMESTAMP,
                    pnl REAL,
                    order_id INTEGER,
                    strategy_id TEXT
                )
            ''')
            conn.commit()

    def save_position(self, symbol, quantity, entry_price, future_symbol, future_price, order_id):
        """Enregistrer la position dans la base de données"""
        try:
            with sqlite3.connect(DB_NAME) as conn:
                cursor = conn.cursor()
                # Enregistrer la jambe action
                cursor.execute('''
                    INSERT INTO positions (symbol, asset_type, quantity, entry_price, entry_time, future_symbol, status, order_id)
                    VALUES (?, 'STOCK', ?, ?, ?, ?, 'OPEN', ?)
                ''', (symbol, quantity, entry_price, datetime.now(), future_symbol, order_id))
                
                # Enregistrer la jambe future
                cursor.execute('''
                    INSERT INTO positions (symbol, asset_type, quantity, entry_price, entry_time, future_symbol, status, order_id)
                    VALUES (?, 'FUTURE', ?, ?, ?, ?, 'OPEN', ?)
                ''', (future_symbol, -1, future_price, datetime.now(), symbol, order_id + 1))
                conn.commit()
                print(f"Position enregistrée pour {symbol}/{future_symbol}")
        except Exception as e:
            print(f"Erreur lors de l'enregistrement de la position: {e}")

    def create_order(self, action, quantity, order_type='MKT'):
        """Créer un ordre de base"""
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type
        order.transmit = True
        return order

    def estimate_commission_whatif(self, contract, action, quantity):
        """Estimer la commission via un ordre what-if IBKR.
        Retourne la commission estimée en devise locale, ou None si échec."""
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = 'MKT'
        order.whatIf = True
        order.transmit = False
        
        order_id = self.ib.next_order_id
        self.ib.next_order_id += 1
        
        self.ib.placeOrder(order_id, contract, order)
        
        # Attendre la réponse what-if (via openOrder callback)
        for _ in range(20):  # 4 secondes max
            if order_id in self.ib.whatif_results:
                break
            time.sleep(0.2)
        
        order_state = self.ib.whatif_results.pop(order_id, None)
        if order_state:
            commission = order_state.commission
            # IBKR utilise 1.7976e+308 comme "non disponible"
            if commission < 1e+308:
                return commission
        return None

    def estimate_spread_commissions(self, stock_contract, future_contract, stock_qty, future_qty):
        """Estimer les commissions totales pour un spread cash & carry (entrée + sortie).
        Utilise what-if IBKR si disponible, sinon fallback sur COST_CONFIG."""
        costs = config.COST_CONFIG
        
        # Tenter what-if pour chaque jambe
        stock_buy_comm = self.estimate_commission_whatif(stock_contract, 'BUY', stock_qty)
        future_sell_comm = self.estimate_commission_whatif(future_contract, 'SELL', future_qty)
        
        if stock_buy_comm is not None and future_sell_comm is not None:
            # What-if = commission pour 1 aller. On double pour l'aller-retour (entrée + sortie).
            total_entry = stock_buy_comm + future_sell_comm
            total_exit = stock_buy_comm + future_sell_comm  # Approximation symétrique
            source = "IBKR what-if"
        else:
            # Fallback sur barème statique
            stock_price_est = 100  # Estimation pour le fallback
            stock_comm = max(stock_qty * stock_price_est * costs['stock_commission_pct'],
                             costs['stock_commission_min'])
            future_comm = future_qty * (costs['future_commission_per_contract'] + costs['future_exchange_fee'])
            total_entry = stock_comm + future_comm
            total_exit = stock_comm + future_comm
            source = "barème statique"
        
        return {
            'entry': total_entry,
            'exit': total_exit,
            'total': total_entry + total_exit,
            'source': source,
            'stock_buy': stock_buy_comm,
            'future_sell': future_sell_comm,
        }

    def place_spread_order(self, stock_contract, future_contract, stock_price, future_price):
        """Placer les ordres pour la stratégie Cash & Carry"""
        # Récupérer le multiplicateur du contrat futur (par défaut 1 si non trouvé ou vide)
        multiplier = 1
        if future_contract.multiplier:
            try:
                multiplier = float(future_contract.multiplier)
            except ValueError:
                pass # Erreur de parsing, on reste à 1
        
        # Pour les actions (SSF) et indices, le ratio est généralement:
        # Qty Action = Qty Future * Multiplier
        
        # On veut vendre 1 contrat Future (ou plus selon la taille max)
        future_qty = 1
        
        # Calculer la quantité d'actions nécessaire pour couvrir 1 future
        stock_quantity = int(future_qty * multiplier)

        # Vérifier si on dépasse la taille max de position autorisée en valeur notionnelle
        notional_value = stock_quantity * stock_price
        max_size = config.STRATEGY_CONFIG['max_position_size']
        
        if notional_value > max_size:
            print(f"Attention: La valeur notionnelle du contrat ({notional_value:.2f}) dépasse la limite ({max_size}).")
            # Dans un cas réel, on pourrait ne pas trader, ou réduire (impossible si min 1 future)
            # Ici on log juste
            
        print(f"Placement des ordres: Achat {stock_quantity} {stock_contract.symbol} / Vente {future_qty} {future_contract.symbol} (Mult: {multiplier})")
        
        # 1. Acheter l'action
        stock_order_id = self.ib.next_order_id
        stock_order = self.create_order('BUY', stock_quantity)
        self.ib.placeOrder(stock_order_id, stock_contract, stock_order)
        self.ib.next_order_id += 1
        
        # 2. Vendre le future
        future_order_id = self.ib.next_order_id
        future_order = self.create_order('SELL', future_qty) # Vente à découvert
        self.ib.placeOrder(future_order_id, future_contract, future_order)
        self.ib.next_order_id += 1
        
        # Enregistrer
        self.save_position(stock_contract.symbol, stock_quantity, stock_price, future_contract.symbol, future_price, stock_order_id)
        return True

    def connect_to_ibkr(self):
        """Se connecter à IBKR TWS"""
        try:
            # Attendre que la connexion soit établie
            max_attempts = 10
            for _ in range(max_attempts):
                if self.ib.connected:
                    self.connected = True
                    print("Connecté à IBKR TWS avec succès")

                    # Configurer le type de données de marché
                    self.ib.reqMarketDataType(config.MARKET_DATA_TYPE)
                    print(f"Type de données de marché configuré: {config.MARKET_DATA_TYPE} ({'Live' if config.MARKET_DATA_TYPE == 1 else 'Delayed/Other'})")

                    # Récupérer les valeurs du compte
                    self.ib.reqAccountUpdates(True, ACCOUNT_ID)

                    # Note: fetch_available_futures() est appelé dans find_cash_carry_opportunities()
                    # avec les bons assets et métadonnées (primary_exchange, derivatives_exchange)

                    # Attendre un peu pour recevoir les données du compte
                    time.sleep(2)

                    # Afficher le solde
                    net_liquidation = self.ib.account_values.get('NetLiquidation', {})
                    print(f"Compte: {ACCOUNT_ID}")
                    print(f"Solde: {net_liquidation.get('value', 'N/A')}")
                    return True
                time.sleep(1)

            print("Échec de la connexion à IBKR TWS")
            return False

        except Exception as e:
            print(f"Erreur de connexion à IBKR: {e}")
            return False

    def run_market_scanner(self):
        """Exécuter le scanner pour trouver des actifs potentiels"""
        print(f"Lancement du scanner de marché pour {CURRENT_MARKET}...")
        
        scan_sub = ScannerSubscription()
        scan_sub.instrument = config.SCANNER_CONFIG['instrument']
        scan_sub.scanCode = config.SCANNER_CONFIG['scanCode']
        scan_sub.numberOfRows = config.SCANNER_CONFIG['numberOfRows']
        
        # Définir la location code
        loc_codes = config.SCANNER_CONFIG['locationCode']
        if CURRENT_MARKET in loc_codes:
            scan_sub.locationCode = loc_codes[CURRENT_MARKET]
        else:
            scan_sub.locationCode = 'STK.US.MAJOR' # Fallback
            
        req_id = 9001
        self.ib.scanner_data[req_id] = [] # Reset previous data
        
        self.ib.reqScannerSubscription(req_id, scan_sub, [], [])
        
        # Attendre les résultats
        print("Attente des résultats du scanner (5s)...")
        time.sleep(5)
        
        # Annuler la souscription pour éviter les mises à jour continues
        self.ib.cancelScannerSubscription(req_id)
        
        results = self.ib.scanner_data.get(req_id, [])
        print(f"Scanner terminé : {len(results)} actifs trouvés.")
        
        assets = []
        for item in results:
            # On suppose par défaut que le symbole du future est le même que l'action 
            # (vrai pour les SSF, faux pour les indices mais le scanner STK retourne des actions)
            symbol = item.contract.symbol
            assets.append({
                'symbol': symbol,
                'future_symbol': symbol, # Pour les SSF, c'est souvent le même ticker
                'name': f"{symbol} Stock"
            })
            
        return assets
    
    def _request_contract_details(self, contract, timeout=10):
        """Demander les détails d'un contrat et ATTENDRE la réponse complète (contractDetailsEnd).
        Retourne la liste des ContractDetails reçus, ou [] si timeout/erreur."""
        req_id = self.ib.get_next_req_id()
        
        # Préparer l'event de synchronisation AVANT d'envoyer la requête
        event = Event()
        self.ib.contract_details_end_events[req_id] = event
        self.ib.contract_details[req_id] = []  # Reset pour ce reqId
        
        # Envoyer la requête
        self.ib.reqContractDetails(req_id, contract)
        
        # Attendre contractDetailsEnd ou erreur (avec timeout)
        event.wait(timeout=timeout)
        
        # Nettoyer l'event
        del self.ib.contract_details_end_events[req_id]
        
        return self.ib.contract_details.get(req_id, [])

    def fetch_available_futures(self, assets_override=None):
        """Récupérer dynamiquement les contrats à terme disponibles depuis IBKR.
        Utilise les métadonnées (primary_exchange, derivatives_exchange) pour qualifier 
        précisément les contrats et éviter les ambiguïtés."""
        print("Récupération des contrats à terme disponibles...")
        
        market_config = config.MARKETS[CURRENT_MARKET]
        
        if assets_override:
             assets = assets_override
        else:
             db = UniverseDatabase()
             currency_filter = market_config['currency']
             db_futures = db.get_all_futures(region_currency=currency_filter)
             assets = []
             seen = set()
             for fut in db_futures:
                 underlying = fut['underlying_symbol']
                 future_sym = fut['symbol']
                 key = (underlying, future_sym)
                 if key in seen or not underlying:
                     continue
                 seen.add(key)
                 assets.append({
                     'symbol': underlying,
                     'future_symbol': future_sym,
                     'name': fut['name'],
                     'primary_exchange': fut.get('underlying_primary_exchange', ''),
                     'derivatives_exchange': fut['derivatives_exchange'],
                 })
        
        if not hasattr(self, 'available_futures'):
            self.available_futures = {}

        # Construire une map dédupliquée : future_symbol -> métadonnées de l'actif
        # On garde la première occurrence pour chaque future_symbol
        futures_to_fetch = {}
        for asset in assets:
            # Ignorer les indices (pas de SSF, flow différent)
            if asset.get('asset_type') == 'INDEX':
                print(f"  [{asset['symbol']}] Index ignoré (pas de SSF)")
                continue
            
            fs = asset['future_symbol']
            if fs and fs not in futures_to_fetch:
                futures_to_fetch[fs] = asset

        for future_symbol, asset in futures_to_fetch.items():
            stock_symbol = asset['symbol']
            primary_exchange = asset.get('primary_exchange', '')
            derivatives_exchange = asset.get('derivatives_exchange', '')
            
            try:
                # ─── Étape 1 : Qualifier le stock (obtenir le bon conId) ───
                stock_contract = self.create_stock_contract(stock_symbol, primary_exchange=primary_exchange)
                stock_details_list = self._request_contract_details(stock_contract, timeout=10)
                
                stock_conid = None
                if stock_details_list:
                    stock_conid = stock_details_list[0].contract.conId
                    resolved_exchange = stock_details_list[0].contract.primaryExchange or stock_details_list[0].contract.exchange
                    print(f"  [{stock_symbol}] Stock qualifié: conId={stock_conid}, exchange={resolved_exchange}")
                else:
                    print(f"  [{stock_symbol}] ERREUR: Impossible de qualifier le stock (primaryExchange={primary_exchange})")
                    continue
                
                # ─── Étape 2 : Rechercher les futures SSF avec échange dérivés explicite ───
                # Table de fallback : si l'échange principal ne retourne rien, essayer les alternatives
                # Priorité : FTA (Euronext Derivatives unifié, confirmé fonctionnel) puis DTB (Eurex)
                FALLBACK_EXCHANGES = {
                    'FTA': ['LSSF', 'DTB'],          # FTA → LSSF → Eurex
                    'LSSF': ['FTA', 'DTB'],          # LSSF → FTA → Eurex
                    'DTB': ['FTA', 'LSSF'],          # Eurex → FTA → LSSF
                    'MONEP': ['FTA', 'LSSF', 'DTB'], # MONEP → FTA → LSSF → Eurex
                }
                
                # Construire la liste d'exchanges à tenter : principal + fallbacks
                exchanges_to_try = [derivatives_exchange] if derivatives_exchange else ['SMART']
                if derivatives_exchange in FALLBACK_EXCHANGES:
                    exchanges_to_try.extend(FALLBACK_EXCHANGES[derivatives_exchange])
                
                future_details_list = []
                used_exchange = exchanges_to_try[0]
                
                for try_exchange in exchanges_to_try:
                    future_contract = self.create_future_contract(
                        future_symbol, 
                        derivatives_exchange=try_exchange
                    )
                    print(f"  [{stock_symbol}] Recherche SSF '{future_symbol}' sur {future_contract.exchange}...")
                    
                    future_details_list = self._request_contract_details(future_contract, timeout=10)
                    
                    if future_details_list:
                        used_exchange = try_exchange
                        if try_exchange != exchanges_to_try[0]:
                            print(f"  [{stock_symbol}] ✓ Trouvé via fallback sur {try_exchange} (primaire {exchanges_to_try[0]} sans résultat)")
                        break
                    else:
                        print(f"  [{stock_symbol}] Aucun contrat future trouvé pour '{future_symbol}' sur {try_exchange}")
                
                if not future_details_list:
                    print(f"  [{stock_symbol}] Aucun SSF trouvé sur aucun exchange ({', '.join(exchanges_to_try)})")
                    continue
                
                print(f"  [{stock_symbol}] {len(future_details_list)} contrat(s) brut(s) reçu(s)")
                
                # ─── Étape 3 : Filtrer — underlyingConId + non expiré ───
                start_date = datetime.now().strftime("%Y%m%d")
                valid_contracts = []
                
                for details in future_details_list:
                    fc = details.contract
                    expiry = fc.lastTradeDateOrContractMonth or ''
                    underlying_conid = getattr(details, 'underConid', 0) or getattr(details, 'underlyingConId', 0)
                    
                    # Filtrer les contrats expirés
                    if not expiry or expiry <= start_date:
                        continue
                    
                    # Vérifier la correspondance sous-jacent
                    if stock_conid and underlying_conid:
                        if underlying_conid != stock_conid:
                            print(f"    [SKIP] {fc.localSymbol} (exp={expiry}): underConId={underlying_conid} != stock conId={stock_conid}")
                            continue
                    
                    # Vérifier la devise
                    if fc.currency != market_config['currency']:
                        print(f"    [SKIP] {fc.localSymbol} (exp={expiry}): devise={fc.currency} != {market_config['currency']}")
                        continue
                    
                    valid_contracts.append(fc)
                
                # ─── Étape 4 : Stocker les contrats valides ───
                if valid_contracts:
                    self.available_futures[future_symbol] = sorted(
                        valid_contracts, 
                        key=lambda c: c.lastTradeDateOrContractMonth
                    )
                    print(f"  [{stock_symbol}] ✓ {len(valid_contracts)} SSF valide(s) :")
                    for c in valid_contracts:
                        print(f"    - {c.lastTradeDateOrContractMonth}: {c.localSymbol} "
                              f"(Mult={c.multiplier}, ConId={c.conId}, Exchange={c.exchange})")
                else:
                    print(f"  [{stock_symbol}] ✗ Aucun SSF valide après filtrage")

            except Exception as e:
                print(f"  [{stock_symbol}] Erreur lors de la récupération: {e}")

    def disconnect_from_ibkr(self):
        """Se déconnecter de IBKR TWS"""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
            print("Déconnecté de IBKR TWS")

    def create_stock_contract(self, symbol, primary_exchange=''):
        """Créer un contrat d'action avec primaryExchange pour désambiguïser le conId"""
        market_config = config.MARKETS[CURRENT_MARKET]
        
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = market_config['currency']
        # primaryExchange est CRITIQUE pour les actions européennes :
        # Sans lui, IBKR peut résoudre vers le mauvais listing (mauvais conId)
        if primary_exchange:
            contract.primaryExchange = primary_exchange
        return contract

    def create_future_contract(self, symbol, expiry='', derivatives_exchange=''):
        """Créer un contrat à terme SSF avec échange dérivés explicite"""
        market_config = config.MARKETS[CURRENT_MARKET]
        
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'FUT'

        # Utiliser l'échange dérivés spécifique à l'actif (MONEP, DTB, FTA, etc.)
        # C'est la correction clé : SMART ne fonctionne pas pour les SSF européens
        if derivatives_exchange:
            contract.exchange = derivatives_exchange
        elif market_config.get('future_exchange'):
            contract.exchange = market_config['future_exchange']
        else:
            contract.exchange = 'SMART'
            
        contract.currency = market_config['currency']
        if expiry:
            contract.lastTradeDateOrContractMonth = expiry

        return contract

    def get_market_price(self, contract, req_id):
        """Récupérer le prix de marché pour un contrat (Snapshot: Bid/Ask/Last)"""
        # Demander les données de marché (Snapshot = True pour éviter flux continu)
        # Snapshot=True est mieux pour éviter de saturer, mais parfois lent.
        # Ici on utilise le snapshot pour éviter l'erreur de limite de lignes de données (max 100)
        self.ib.reqMktData(req_id, contract, '', True, False, [])

        # Attendre la réception des données (Polling avec timeout)
        # On attend jusqu'à 4 secondes max, mais on sort dès qu'on a des données significatives
        # 4 = Last, 66 = DelayedLast, 1 = Bid, 67 = DelayedBid, 2 = Ask, 68 = DelayedAsk, 9 = Close, 75 = DelayedClose
        significant_ticks = [4, 66, 1, 67, 2, 68, 9, 75] 
        
        for _ in range(20): # 20 * 0.2s = 4 secondes max
            if req_id in self.ib.market_data:
                data = self.ib.market_data[req_id]
                # Vérifier si on a au moins une donnée utile
                if any(t in data for t in significant_ticks):
                    break
            time.sleep(0.2)
        
        # Récupérer les prix
        price_data = {'last': None, 'bid': None, 'ask': None}

        if req_id in self.ib.market_data:
            data = self.ib.market_data[req_id]
            
            # 1. Essayer LAST (Temps réel ou Différé)
            price_data['last'] = data.get(4) or data.get(66)
            
            # 2. Essayer BID/ASK (Temps réel ou Différé)
            price_data['bid'] = data.get(1) or data.get(67)
            price_data['ask'] = data.get(2) or data.get(68)
            
            # 3. Si pas de LAST, essayer de construire un prix médian BID/ASK
            if not price_data['last'] and price_data['bid'] and price_data['ask']:
                price_data['last'] = (price_data['bid'] + price_data['ask']) / 2
                
            # 4. Si toujours rien, essayer CLOSE (Temps réel ou Différé)
            if not price_data['last']:
                price_data['last'] = data.get(9) or data.get(75)

            # Debug si toujours rien
            if not price_data['last']:
                 print(f"    [DEBUG] Pas de prix trouvé pour {contract.symbol}. Ticks reçus: {list(data.keys())}")
        else:
             print(f"    [DEBUG] Aucune donnée reçue pour {contract.symbol} (reqId {req_id})")

        # NOTE: Avec snapshot=True, IBKR ferme automatiquement la souscription une fois les données envoyées.
        # Appeler cancelMktData ici provoque l'erreur 300 "Can't find EId" car la reqId n'existe plus.
        # On ne l'appelle donc pas.
            
        return price_data

    def find_cash_carry_opportunities(self, use_scanner=False):
        """Trouver des opportunités Cash and Carry"""
        if not self.connected:
            print("Non connecté à IBKR")
            return []

        opportunities = []
        
        if use_scanner:
            print("Utilisation du scanner dynamique pour trouver des actifs...")
            scanned_assets = self.run_market_scanner()
            # On met à jour les futures disponibles pour ces nouveaux actifs
            self.fetch_available_futures(assets_override=scanned_assets)
            assets = scanned_assets
        else:
            # Charger les futures depuis la base de données univers
            db = UniverseDatabase()
            currency_filter = config.MARKETS[CURRENT_MARKET]['currency']
            print(f"Chargement des futures depuis la base de données (Devise: {currency_filter})...")
            
            db_futures = db.get_all_futures(region_currency=currency_filter)
            
            # Construire la liste d'assets au format attendu par fetch_available_futures
            # Chaque future a un underlying_symbol qu'on utilise comme stock symbol
            assets = []
            seen = set()
            for fut in db_futures:
                underlying = fut['underlying_symbol']
                future_sym = fut['symbol']
                key = (underlying, future_sym)
                if key in seen or not underlying:
                    continue
                seen.add(key)
                assets.append({
                    'symbol': underlying,
                    'future_symbol': future_sym,
                    'name': fut['name'],
                    'primary_exchange': fut.get('underlying_primary_exchange', ''),
                    'derivatives_exchange': fut['derivatives_exchange'],
                })
            print(f"  -> {len(assets)} futures chargés depuis universe.db")
            
            if not assets:
                print("  -> Aucun future en base pour cette devise. Lancez d'abord update_universe.py")
                return []
            
            # Récupérer les contrats futures disponibles sur IBKR
            self.fetch_available_futures(assets_override=assets)
        
        print(f"\n{'='*80}")
        print(f"  ANALYSE DES OPPORTUNITÉS CASH & CARRY — {CURRENT_MARKET} ({len(assets)} actifs)")
        print(f"{'='*80}")

        for asset_idx, asset in enumerate(assets):
            stock_symbol = asset['symbol']
            future_symbol = asset.get('future_symbol', '')
            primary_exchange = asset.get('primary_exchange', '')
            
            # Ignorer les indices
            if asset.get('asset_type') == 'INDEX':
                continue
            
            try:
                # 1. Vérifier que des futures ont été trouvées pour cet actif
                if not hasattr(self, 'available_futures') or future_symbol not in self.available_futures:
                    print(f"\n  [{stock_symbol}] Aucun contrat futur trouvé ({future_symbol})")
                    continue

                futures_list = self.available_futures[future_symbol]
                
                if not futures_list:
                    print(f"\n  [{stock_symbol}] Liste de futures vide")
                    continue

                # Créer le contrat action avec primaryExchange
                stock_contract = self.create_stock_contract(stock_symbol, primary_exchange=primary_exchange)
                derivatives_exchange = asset.get('derivatives_exchange', '')
                
                # Récupérer le prix de l'action (reqId unique via compteur)
                req_id_stock = self.ib.get_next_req_id()
                stock_data = self.get_market_price(stock_contract, req_id_stock)

                # Pour le cash & carry, on ACHÈTE l'action → prix exécutable = Ask
                stock_ask = stock_data['ask']
                stock_bid = stock_data['bid']
                stock_last = stock_data['last']
                
                if not stock_ask:
                    if stock_last:
                        stock_ask = stock_last
                    elif stock_bid:
                        stock_ask = stock_bid
                    else:
                        print(f"\n  [{stock_symbol}] Pas de prix action (Last/Bid/Ask manquants)")
                        continue

                stock_mid = (stock_bid + stock_ask) / 2 if stock_bid and stock_ask else stock_ask

                print(f"\n{'─'*80}")
                print(f"  {stock_symbol} — Prix Action: Bid={stock_bid}  Ask={stock_ask}  Last={stock_last}")
                print(f"{'─'*80}")

                # Déterminer la quantité (multiplicateur du SSF, généralement 100 pour les SSF Euronext)
                multiplier = 1

                # Itérer sur CHAQUE maturité disponible
                for i, future_contract in enumerate(futures_list):
                    expiry = future_contract.lastTradeDateOrContractMonth
                    
                    # Récupérer le multiplicateur réel du contrat
                    if future_contract.multiplier:
                        try:
                            multiplier = int(float(future_contract.multiplier))
                        except (ValueError, TypeError):
                            multiplier = 1
                    
                    stock_qty = multiplier  # Nombre d'actions pour couvrir 1 contrat
                    future_qty = 1
                    
                    # Récupérer prix du future (reqId unique via compteur)
                    req_id_future = self.ib.get_next_req_id()
                    future_data = self.get_market_price(future_contract, req_id_future)
                    
                    # On VEND le future → prix exécutable = Bid
                    future_bid = future_data['bid']
                    future_ask = future_data['ask']
                    future_last = future_data['last']
                    
                    if not future_bid:
                        if future_last:
                            future_bid = future_last
                        elif future_ask:
                            future_bid = future_ask
                    
                    if not future_bid:
                        print(f"  Maturité {expiry}: Pas de prix future disponible")
                        continue

                    # Prix exécutables : on achète au Ask, on vend au Bid
                    exec_stock_price = stock_ask
                    exec_future_price = future_bid

                    # Sanity check : prix cohérent (SSF ≈ prix action, tolérance 20%)
                    price_diff_pct = abs(exec_future_price - exec_stock_price) / exec_stock_price
                    if price_diff_pct > 0.20:
                        print(f"    [WARNING] Prix incohérent pour {stock_symbol} ({expiry}). "
                              f"Action={exec_stock_price:.2f}, Future={exec_future_price:.2f} (écart={price_diff_pct*100:.1f}%). Ignoré.")
                        continue

                    # Calculer le spread exécutable (Bid future - Ask action)
                    spread = exec_future_price - exec_stock_price

                    # Calculer tous les coûts
                    cost_detail = self.calculate_total_cost(
                        stock_price=exec_stock_price,
                        stock_qty=stock_qty,
                        future_qty=future_qty,
                        expiry=expiry,
                        stock_contract=stock_contract,
                        future_contract=future_contract,
                        derivatives_exchange=derivatives_exchange,
                    )
                    
                    total_cost = cost_detail['total_cost']
                    spread_pct = (spread / exec_stock_price) * 100
                    cost_pct = cost_detail['total_cost_pct']
                    profit_net = spread - total_cost
                    profit_net_pct = (profit_net / exec_stock_price) * 100
                    # Annualiser le rendement
                    annual_return_pct = profit_net_pct / cost_detail['T'] if cost_detail['T'] > 0 else 0
                    is_opportunity = spread > total_cost * config.STRATEGY_CONFIG['min_spread_ratio']

                    print(f"  Maturité {expiry} ({future_contract.localSymbol}, Mult={multiplier}, Exchange={future_contract.exchange}):")
                    print(f"    Action (achat) : {exec_stock_price:>10.2f}  (Bid={stock_bid}  Ask={stock_ask}  Last={stock_last})")
                    print(f"    Future (vente) : {exec_future_price:>10.2f}  (Bid={future_bid}  Ask={future_ask}  Last={future_last})")
                    print(f"    Spread exec.   : {spread:>+10.2f}  ({spread_pct:>+.2f}%)")
                    print(f"    --- Détail des coûts ({cost_detail['days']}j, comm={cost_detail['commission_source']}) ---")
                    print(f"    Financement    : {cost_detail['funding_cost']:>10.2f}  ({cost_detail['funding_rate']*100:.2f}% annuel)")
                    print(f"    Dividendes est.: {cost_detail['dividend_benefit']:>10.2f}  (bénéfice)")
                    print(f"    Commissions    : {cost_detail['commission_per_share']:>10.2f}  (total={cost_detail['commission_total']:.2f})")
                    print(f"    Slippage est.  : {cost_detail['slippage_per_share']:>10.2f}")
                    print(f"    TTF/taxes      : {cost_detail['ftt_per_share']:>10.2f}")
                    print(f"    Coût total     : {total_cost:>10.2f}  ({cost_pct:>+.2f}%)")
                    print(f"    Profit net est.: {profit_net:>+10.2f}  ({profit_net_pct:>+.2f}%,  annualisé={annual_return_pct:>+.1f}%)")
                    print(f"    Rentabilité    : {'>>> OPPORTUNITÉ <<<' if is_opportunity else 'Insuffisante'}")

                    if is_opportunity:
                        opportunities.append({
                            'stock': stock_symbol,
                            'stock_price': exec_stock_price,
                            'future': future_symbol,
                            'future_price': exec_future_price,
                            'expiry': expiry,
                            'spread': spread,
                            'total_cost': total_cost,
                            'profit_net': profit_net,
                            'profit_net_pct': profit_net_pct,
                            'annual_return_pct': annual_return_pct,
                            'stock_contract': stock_contract,
                            'future_contract': future_contract,
                            'cost_detail': cost_detail,
                        })

            except Exception as e:
                print(f"\n  [{stock_symbol}] Erreur: {e}")

        # Résumé final
        print(f"\n{'='*80}")
        print(f"  RÉSUMÉ : {len(opportunities)} opportunité(s) sur {len(assets)} actifs analysés")
        print(f"{'='*80}")
        if opportunities:
            for opp in opportunities:
                print(f"  {opp['stock']:>6s}  Action={opp['stock_price']:.2f}  Future={opp['future_price']:.2f}  "
                      f"Spread={opp['spread']:+.2f}  Profit={opp['profit_net']:+.2f} ({opp['profit_net_pct']:+.2f}%, ann.={opp['annual_return_pct']:+.1f}%)  "
                      f"Exp={opp['expiry']}")
        else:
            print("  Aucune opportunité détectée.")
        print(f"{'='*80}\n")

        return opportunities

    def calculate_total_cost(self, stock_price, stock_qty, future_qty, expiry,
                              stock_contract, future_contract, derivatives_exchange=''):
        """Calculer le coût total du cash & carry (par action) pour déterminer le spread minimum.
        
        Modèle: Profit = (F - S) - coût_total
        coût_total = financement + commissions + slippage + taxes - dividendes
        
        Args:
            stock_price: Prix de l'action
            stock_qty: Quantité d'actions
            future_qty: Quantité de contrats futures
            expiry: Date d'expiration YYYYMMDD
            stock_contract: Contrat IBKR de l'action
            future_contract: Contrat IBKR du future
            derivatives_exchange: Exchange du future (pour identifier les taxes)
        
        Returns:
            dict avec le détail des coûts et le spread minimum nécessaire
        """
        costs = config.COST_CONFIG
        
        # ── 1. Temps jusqu'à expiration ──
        if expiry:
            try:
                expiry_date = datetime.strptime(expiry, "%Y%m%d")
                days_to_expiry = max((expiry_date - datetime.now()).days, 1)
                T = days_to_expiry / 365.0
            except (ValueError, TypeError):
                T = 0.25
                days_to_expiry = 91
        else:
            T = 0.25
            days_to_expiry = 91

        # ── 2. Coût de financement (achat action sur marge) ──
        funding_rate = self.ib.get_funding_rate()
        funding_cost = stock_price * funding_rate * T

        # ── 3. Dividendes estimés (réduction du coût) ──
        div_yield = costs['dividend_yield_default']
        dividend_benefit = stock_price * div_yield * T

        # ── 4. Commissions (what-if IBKR ou fallback statique) ──
        comm = self.estimate_spread_commissions(
            stock_contract, future_contract, stock_qty, future_qty
        )
        # Ramener les commissions totales à un coût par action
        commission_per_share = comm['total'] / stock_qty if stock_qty > 0 else 0

        # ── 5. Slippage (bid-ask estimé, 2 jambes × 2 allers-retours) ──
        slippage_per_share = stock_price * (costs['slippage_bps'] / 10000) * 4  # 4 traversées de spread

        # ── 6. Taxes sur transactions financières ──
        ftt_per_share = 0.0
        # France: TTF 0.3% à l'achat (actions FR, capitalisation > 1Md€)
        if derivatives_exchange in ('DPAR', 'FTA') or (hasattr(stock_contract, 'primaryExchange') and stock_contract.primaryExchange == 'SBF'):
            ftt_per_share = stock_price * costs['ftt_rate_fr']
        # Italie: TTF 0.1%
        elif derivatives_exchange in ('DMIL',):
            ftt_per_share = stock_price * costs['ftt_rate_it']

        # ── Coût total par action ──
        total_cost = funding_cost + commission_per_share + slippage_per_share + ftt_per_share - dividend_benefit

        return {
            'T': T,
            'days': days_to_expiry,
            'funding_rate': funding_rate,
            'funding_cost': funding_cost,
            'dividend_benefit': dividend_benefit,
            'commission_total': comm['total'],
            'commission_per_share': commission_per_share,
            'commission_source': comm['source'],
            'slippage_per_share': slippage_per_share,
            'ftt_per_share': ftt_per_share,
            'total_cost': total_cost,
            'total_cost_pct': (total_cost / stock_price) * 100 if stock_price else 0,
        }

    def run_trading_session(self):
        """Exécuter une session de trading complète"""
        try:
            # Se connecter à IBKR
            if not self.connect_to_ibkr():
                return

            # Rechercher des opportunités
            use_scanner = False 
            opportunities = self.find_cash_carry_opportunities(use_scanner=use_scanner)

            if opportunities:
                # 1. Trier par rendement annualisé décroissant
                opportunities.sort(key=lambda x: x['annual_return_pct'], reverse=True)
                
                # 2. Sélectionner les N meilleures
                max_trades = getattr(config, 'MAX_TRADES_PER_SESSION', 1)
                best_opportunities = opportunities[:max_trades]
                
                print(f"\n{'='*80}")
                print(f"  SÉLECTION DES {len(best_opportunities)} MEILLEURE(S) OPPORTUNITÉ(S)")
                print(f"{'='*80}")

                for opp in best_opportunities:
                    print(f"\n>>> CANDIDAT : {opp['stock']} <<<")
                    print(f"    Action={opp['stock_price']:.2f}  Future={opp['future_price']:.2f}  "
                          f"Spread={opp['spread']:+.2f}  Coût={opp['total_cost']:.2f}  "
                          f"Profit={opp['profit_net']:+.2f} ({opp['profit_net_pct']:+.2f}%, ann.={opp['annual_return_pct']:+.1f}%)")
                    
                    # 3. Vérifier le flag d'exécution
                    if getattr(config, 'EXECUTE_ORDERS', False):
                        print("    [EXECUTION] Placement des ordres réels...")
                        self.place_spread_order(
                            opp['stock_contract'], 
                            opp['future_contract'], 
                            opp['stock_price'], 
                            opp['future_price']
                        )
                    else:
                        print("    [SIMULATION] Ordres NON exécutés (EXECUTE_ORDERS=False).")
                        print(f"    -> Achat {opp['stock']} / Vente {opp['future']}")

                    time.sleep(5)

            print("\nSession de trading terminée avec succès")

        except Exception as e:
            print(f"Erreur lors de la session de trading: {e}")

        finally:
            # Se déconnecter
            self.disconnect_from_ibkr()

    def set_market(self, market_region):
        """Changer de marché/region pour le trading"""
        if market_region in config.MARKETS and config.MARKETS[market_region]['enabled']:
            global CURRENT_MARKET
            CURRENT_MARKET = market_region
            print(f"Marché changé vers: {market_region}")
            return True
        else:
            print(f"Erreur: Région de marché invalide ou désactivée '{market_region}'.")
            return False

def main():
    """Fonction principale"""
    print("Démarrage du programme Cash and Carry pour IBKR (API native)")
    print(f"Mode: {'TEMPS RÉEL' if USE_REAL_TIME_DATA else 'DIFFÉRÉ (Demo)'}")
    print(f"Compte: {ACCOUNT_ID}")
    print(f"Chemin TWS: {TWS_PATH}")
    print("Utilisation de l'API native IBKR depuis C:/TWS API/source/pythonclient")
    print(f"Données de marché: {DATA_MODE}")

    trader = CashCarryTrader()

    # Exécuter une session de trading
    trader.run_trading_session()

if __name__ == "__main__":
    main()
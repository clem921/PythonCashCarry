#!/usr/bin/env python3
"""
Script pour mettre à jour les informations de dividendes dans la base de données.
Utilise yfinance pour récupérer les dates ex-dividende, dates de paiement et montants.

Usage:
    python update_dividends.py              # Met à jour tous les actifs de la base
    python update_dividends.py --symbol MC  # Met à jour un symbole spécifique
    python update_dividends.py --currency EUR  # Met à jour les actifs en EUR uniquement
"""

import argparse
import pandas as pd
import time
from datetime import datetime
from universe_manager import UniverseDatabase

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
        help='Afficher uniquement les dividendes à venir (pas de mise à jour)'
    )
    
    args = parser.parse_args()
    
    db = UniverseDatabase()
    
    if args.show:
        show_upcoming_dividends(db, args.currency)
    elif args.symbol:
        update_single_dividend(db, args.symbol, args.currency or 'EUR')
    else:
        update_all_dividends(db, args.currency, args.type)
        print("\n")
        show_upcoming_dividends(db, args.currency)


if __name__ == "__main__":
    main()
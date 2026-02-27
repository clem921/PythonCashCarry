import sqlite3
import pandas as pd
from datetime import datetime
import config

DB_NAME = "universe.db"

class UniverseDatabase:
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name
        self.init_db()

    def init_db(self):
        """Initialiser la base de données de l'univers avec la table managed_assets"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS managed_assets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    sec_type TEXT DEFAULT 'STK',
                    exchange TEXT DEFAULT 'SMART',
                    currency TEXT DEFAULT 'EUR',
                    future_symbol TEXT,
                    name TEXT,
                    source TEXT,
                    active BOOLEAN DEFAULT 1,
                    last_updated TIMESTAMP,
                    primary_exchange TEXT DEFAULT '',
                    derivatives_exchange TEXT DEFAULT '',
                    product_type TEXT DEFAULT 'STK',
                    underlying_symbol TEXT DEFAULT '',
                    contract_size INTEGER DEFAULT 1,
                    multiplier INTEGER DEFAULT 1,
                    expirations TEXT DEFAULT ''
                )
            ''')
            # Index unique sur (symbol, exchange, currency) pour éviter les doublons
            cursor.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_asset 
                ON managed_assets (symbol, exchange, currency)
            ''')
            # Index pour les futures
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_future_symbol 
                ON managed_assets (future_symbol) WHERE future_symbol IS NOT NULL
            ''')
            # Index pour les sous-jacents
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_underlying_symbol 
                ON managed_assets (underlying_symbol) WHERE underlying_symbol IS NOT NULL
            ''')
            
            # Migration : ajouter les nouvelles colonnes si elles n'existent pas encore
            new_columns = [
                ('product_type', 'TEXT DEFAULT \'STK\''),
                ('underlying_symbol', 'TEXT DEFAULT \'\''),
                ('contract_size', 'INTEGER DEFAULT 1'),
                ('multiplier', 'INTEGER DEFAULT 1'),
                ('expirations', 'TEXT DEFAULT \'\''),
                ('underlying_isin', 'TEXT DEFAULT \'\''),
                ('underlying_primary_exchange', 'TEXT DEFAULT \'\''),
                ('ex_dividend_date', 'TEXT DEFAULT \'\''),
                ('payment_date', 'TEXT DEFAULT \'\''),
                ('dividend_amount', 'REAL DEFAULT 0'),
                ('derivative_category', 'TEXT DEFAULT \'\''),
                ('yfinance_available', 'INTEGER DEFAULT 1')
            ]
            
            for col_name, col_def in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE managed_assets ADD COLUMN {col_name} {col_def}")
                    print(f"  -> Colonne {col_name} ajoutée")
                except Exception:
                    pass  # La colonne existe déjà
            
            conn.commit()

    def add_asset(self, symbol, future_symbol, name, exchange='SMART', currency='EUR', sec_type='STK', source='MANUAL', primary_exchange='', derivatives_exchange='', product_type='STK', underlying_symbol='', contract_size=1, multiplier=1, expirations='', underlying_isin='', underlying_primary_exchange='', derivative_category=''):
        """Ajouter ou mettre à jour un actif dans l'univers"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO managed_assets (symbol, sec_type, exchange, currency, future_symbol, name, source, active, last_updated, primary_exchange, derivatives_exchange, product_type, underlying_symbol, contract_size, multiplier, expirations, underlying_isin, underlying_primary_exchange, derivative_category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, exchange, currency) DO UPDATE SET
                        future_symbol=excluded.future_symbol,
                        name=excluded.name,
                        source=excluded.source,
                        active=1,
                        last_updated=excluded.last_updated,
                        primary_exchange=excluded.primary_exchange,
                        derivatives_exchange=excluded.derivatives_exchange,
                        product_type=excluded.product_type,
                        underlying_symbol=excluded.underlying_symbol,
                        contract_size=excluded.contract_size,
                        multiplier=excluded.multiplier,
                        expirations=excluded.expirations,
                        underlying_isin=excluded.underlying_isin,
                        underlying_primary_exchange=excluded.underlying_primary_exchange,
                        derivative_category=excluded.derivative_category
                ''', (symbol, sec_type, exchange, currency, future_symbol, name, source, datetime.now(), primary_exchange, derivatives_exchange, product_type, underlying_symbol, contract_size, multiplier, expirations, underlying_isin, underlying_primary_exchange, derivative_category))
                conn.commit()
                return True
            except Exception as e:
                print(f"Erreur lors de l'ajout de {symbol}: {e}")
                return False

    def add_future_asset(self, symbol, future_symbol, name, underlying_symbol, exchange='SMART', currency='EUR', source='MANUAL', derivatives_exchange='', underlying_primary_exchange='', contract_size=1, multiplier=1, expirations='', underlying_isin='', derivative_category=''):
        """
        Ajouter un actif future avec son sous-jacent.
        primary_exchange du future = derivatives_exchange (là où le contrat se négocie).
        underlying_primary_exchange = place de cotation du sous-jacent (SBF, AEB, IBIS…).
        """
        return self.add_asset(
            symbol=symbol,
            future_symbol=future_symbol,
            name=name,
            exchange=exchange,
            currency=currency,
            sec_type='FUT',
            source=source,
            primary_exchange=derivatives_exchange,
            derivatives_exchange=derivatives_exchange,
            product_type='FUT',
            underlying_symbol=underlying_symbol,
            contract_size=contract_size,
            multiplier=multiplier,
            expirations=expirations,
            underlying_isin=underlying_isin,
            underlying_primary_exchange=underlying_primary_exchange,
            derivative_category=derivative_category
        )

    def get_asset_by_symbol(self, symbol, exchange=None, currency=None):
        """Récupérer un actif par son symbole (et optionnellement exchange/currency pour lever l'ambiguïté)"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT symbol, future_symbol, exchange, currency, name, primary_exchange, derivatives_exchange, product_type, underlying_symbol, contract_size, multiplier, underlying_primary_exchange, derivative_category
                FROM managed_assets WHERE symbol = ? AND active = 1
            '''
            params = [symbol]
            if exchange:
                query += ' AND exchange = ?'
                params.append(exchange)
            if currency:
                query += ' AND currency = ?'
                params.append(currency)
            query += ' ORDER BY last_updated DESC LIMIT 1'
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            if row:
                return {
                    'symbol': row[0],
                    'future_symbol': row[1],
                    'exchange': row[2],
                    'currency': row[3],
                    'name': row[4],
                    'primary_exchange': row[5] or '',
                    'derivatives_exchange': row[6] or '',
                    'product_type': row[7] or 'STK',
                    'underlying_symbol': row[8] or '',
                    'contract_size': row[9] or 1,
                    'multiplier': row[10] or 1,
                    'underlying_primary_exchange': row[11] or '',
                    'derivative_category': row[12] or ''
                }
            return None

    def get_futures_for_underlying(self, underlying_symbol):
        """Récupérer tous les futures pour un sous-jacent donné"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT symbol, future_symbol, exchange, currency, name, derivatives_exchange, contract_size, multiplier
                FROM managed_assets 
                WHERE underlying_symbol = ? AND product_type = 'FUT' AND active = 1
                ORDER BY symbol
            ''', (underlying_symbol,))
            rows = cursor.fetchall()
            
            futures = []
            for row in rows:
                futures.append({
                    'symbol': row[0],
                    'future_symbol': row[1],
                    'exchange': row[2],
                    'currency': row[3],
                    'name': row[4],
                    'derivatives_exchange': row[5] or '',
                    'contract_size': row[6] or 1,
                    'multiplier': row[7] or 1
                })
            return futures

    def get_all_futures(self, region_currency=None):
        """Récupérer tous les contrats futures, optionnellement filtrés par devise"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT symbol, underlying_symbol, exchange, currency, name, derivatives_exchange, contract_size, multiplier, expirations, underlying_isin, primary_exchange, underlying_primary_exchange
                FROM managed_assets 
                WHERE product_type = 'FUT' AND active = 1
            '''
            params = []
            if region_currency:
                query += ' AND currency = ?'
                params.append(region_currency)
            query += ' ORDER BY underlying_symbol, symbol'
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            futures = []
            for row in rows:
                futures.append({
                    'symbol': row[0],
                    'underlying_symbol': row[1],
                    'exchange': row[2],
                    'currency': row[3],
                    'name': row[4],
                    'derivatives_exchange': row[5] or '',
                    'contract_size': row[6] or 1,
                    'multiplier': row[7] or 1,
                    'expirations': row[8] or '',
                    'underlying_isin': row[9] or '',
                    'primary_exchange': row[10] or '',
                    'underlying_primary_exchange': row[11] or '',
                })
            return futures

    def get_assets_with_futures(self):
        """Récupérer tous les actifs qui ont des futures associés"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT a.symbol, a.future_symbol, a.exchange, a.currency, a.name, a.primary_exchange, a.derivatives_exchange
                FROM managed_assets a
                WHERE a.future_symbol IS NOT NULL AND a.future_symbol != '' AND a.active = 1
                ORDER BY a.symbol
            ''')
            rows = cursor.fetchall()
            
            assets = []
            for row in rows:
                assets.append({
                    'symbol': row[0],
                    'future_symbol': row[1],
                    'exchange': row[2],
                    'currency': row[3],
                    'name': row[4],
                    'primary_exchange': row[5] or '',
                    'derivatives_exchange': row[6] or ''
                })
            return assets

    def get_active_assets(self, region_currency=None, category=None):
        """Récupérer tous les actifs actifs, optionnellement filtrés par devise ou catégorie"""
        with sqlite3.connect(self.db_name) as conn:
            query = "SELECT symbol, underlying_symbol, future_symbol, exchange, currency, name, primary_exchange, derivatives_exchange, sec_type, derivative_category, underlying_primary_exchange, yfinance_available FROM managed_assets WHERE active = 1"
            params = []
            
            if region_currency:
                query += " AND currency = ?"
                params.append(region_currency)
                
            if category:
                query += " AND derivative_category = ?"
                params.append(category)
                
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            assets = []
            for row in rows:
                assets.append({
                    'symbol': row[0],
                    'underlying_symbol': row[1] or '',
                    'future_symbol': row[2],
                    'exchange': row[3],
                    'currency': row[4],
                    'name': row[5],
                    'primary_exchange': row[6] or '',
                    'derivatives_exchange': row[7] or '',
                    'sec_type': row[8] or 'STK',
                    'derivative_category': row[9] or '',
                    'underlying_primary_exchange': row[10] or '',
                    'yfinance_available': row[11] if row[11] is not None else 1
                })
            return assets

    def deactivate_all_from_source(self, source):
        """Désactiver tous les assets d'une source avant mise à jour (pour gérer les suppressions)"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE managed_assets SET active = 0 WHERE source = ?", (source,))
            conn.commit()

    def get_futures_mapping(self):
        """Récupérer le mapping futures -> sous-jacents"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT underlying_symbol, symbol, derivatives_exchange, contract_size, multiplier
                FROM managed_assets 
                WHERE product_type = 'FUT' AND active = 1
                ORDER BY underlying_symbol
            ''')
            rows = cursor.fetchall()
            
            mapping = {}
            for row in rows:
                underlying = row[0]
                future_symbol = row[1]
                exchange = row[2] or ''
                contract_size = row[3] or 1
                multiplier = row[4] or 1
                
                if underlying not in mapping:
                    mapping[underlying] = []
                
                mapping[underlying].append({
                    'future_symbol': future_symbol,
                    'exchange': exchange,
                    'contract_size': contract_size,
                    'multiplier': multiplier
                })
            
            return mapping

    def validate_futures_contracts(self):
        """Valider les contrats futures"""
        print("Validation des contrats futures...")
        
        futures = self.get_all_futures()
        valid_futures = []
        
        for future in futures:
            # Vérification basique
            if (future['symbol'] and 
                len(future['symbol']) <= 6 and 
                future['symbol'].isalnum() and
                future['underlying_symbol']):
                
                valid_futures.append(future)
        
        print(f"  -> {len(valid_futures)} contrats valides sur {len(futures)}")
        return valid_futures

    def update_dividend(self, symbol, ex_dividend_date='', payment_date='', dividend_amount=0, exchange='SMART', currency='EUR', yfinance_available=1):
        """
        Mettre à jour les informations de dividende pour un actif
        
        Args:
            symbol: Symbole de l'actif
            ex_dividend_date: Date ex-dividende (format YYYY-MM-DD ou vide)
            payment_date: Date de paiement (format YYYY-MM-DD ou vide)
            dividend_amount: Montant du dividende
            exchange: Exchange de l'actif (défaut: SMART)
            currency: Devise (défaut: EUR)
            yfinance_available: Si l'actif est disponible dans yfinance (1: oui, 0: non)
        
        Returns:
            bool: True si mise à jour réussie, False sinon
        """
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    UPDATE managed_assets 
                    SET ex_dividend_date = ?, payment_date = ?, dividend_amount = ?, yfinance_available = ?, last_updated = ?
                    WHERE symbol = ? AND exchange = ? AND currency = ?
                ''', (ex_dividend_date, payment_date, dividend_amount, yfinance_available, datetime.now(), symbol, exchange, currency))
                if cursor.rowcount == 0:
                    print(f"[DB] update_dividend: 0 lignes mises à jour pour {symbol} (exchange={exchange}, currency={currency})")
                    conn.commit()
                    return False
                conn.commit()
                return True
            except Exception as e:
                print(f"ERREUR lors de la mise à jour du dividende pour {symbol}: {e}")
                return False

    def get_dividend(self, symbol, exchange='SMART', currency='EUR'):
        """Récupérer les informations de dividende pour un actif
        
        Args:
            symbol: Symbole de l'actif
            exchange: Exchange de l'actif (défaut: SMART)
            currency: Devise (défaut: EUR)
        
        Returns:
            dict avec ex_dividend_date, payment_date, dividend_amount ou None si non trouvé
        """
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ex_dividend_date, payment_date, dividend_amount
                FROM managed_assets 
                WHERE symbol = ? AND exchange = ? AND currency = ? AND active = 1
            ''', (symbol, exchange, currency))
            row = cursor.fetchone()
            
            if row:
                return {
                    'symbol': symbol,
                    'ex_dividend_date': row[0] or '',
                    'payment_date': row[1] or '',
                    'dividend_amount': row[2] or 0
                }
            return None

    def get_all_dividends(self, region_currency=None):
        """Récupérer tous les dividendes enregistrés
        
        Args:
            region_currency: Filtrer par devise (optionnel)
        
        Returns:
            Liste de dicts avec les infos de dividende
        """
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT symbol, exchange, currency, ex_dividend_date, payment_date, dividend_amount
                FROM managed_assets 
                WHERE active = 1 AND ex_dividend_date IS NOT NULL AND ex_dividend_date != ''
            '''
            params = []
            if region_currency:
                query += ' AND currency = ?'
                params.append(region_currency)
            query += ' ORDER BY ex_dividend_date'
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            dividends = []
            for row in rows:
                dividends.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'currency': row[2],
                    'ex_dividend_date': row[3],
                    'payment_date': row[4],
                    'dividend_amount': row[5]
                })
            return dividends

    def get_upcoming_dividends(self, days_ahead=90, region_currency=None):
        """Récupérer les dividendes à venir dans les N prochains jours
        
        Args:
            days_ahead: Nombre de jours à regarder (défaut: 90)
            region_currency: Filtrer par devise (optionnel)
        
        Returns:
            Liste de dicts avec les dividendes à venir
        """
        from datetime import timedelta
        
        today = datetime.now().strftime('%Y-%m-%d')
        future_date = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            query = '''
                SELECT symbol, exchange, currency, ex_dividend_date, payment_date, dividend_amount
                FROM managed_assets 
                WHERE active = 1 
                AND ex_dividend_date IS NOT NULL 
                AND ex_dividend_date != ''
                AND ex_dividend_date >= ?
                AND ex_dividend_date <= ?
            '''
            params = [today, future_date]
            if region_currency:
                query += ' AND currency = ?'
                params.append(region_currency)
            query += ' ORDER BY ex_dividend_date'
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            dividends = []
            for row in rows:
                dividends.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'currency': row[2],
                    'ex_dividend_date': row[3],
                    'payment_date': row[4],
                    'dividend_amount': row[5]
                })
            return dividends

    def get_summary(self):
        """Obtenir un résumé de la base de données"""
        with sqlite3.connect(self.db_name) as conn:
            cursor = conn.cursor()
            
            # Compter les actifs par type
            cursor.execute('''
                SELECT product_type, COUNT(*) as count
                FROM managed_assets WHERE active = 1
                GROUP BY product_type
            ''')
            type_counts = cursor.fetchall()
            
            # Compter les futures par devise
            cursor.execute('''
                SELECT currency, COUNT(*) as count
                FROM managed_assets WHERE product_type = 'FUT' AND active = 1
                GROUP BY currency
            ''')
            currency_counts = cursor.fetchall()
            
            # Compter les actifs avec futures
            cursor.execute('''
                SELECT COUNT(DISTINCT symbol) as count
                FROM managed_assets WHERE future_symbol IS NOT NULL AND future_symbol != '' AND active = 1
            ''')
            assets_with_futures = cursor.fetchone()[0]
            
            return {
                'type_counts': dict(type_counts),
                'currency_counts': dict(currency_counts),
                'assets_with_futures': assets_with_futures,
                'total_active': sum(count for _, count in type_counts)
            }

if __name__ == "__main__":
    # Test simple
    db = UniverseDatabase()
    db.add_asset('SAN', 'SAN', 'Sanofi', exchange='SMART', currency='EUR', source='TEST')
    print("Assets in DB:", db.get_active_assets())
    
    # Test des nouvelles fonctionnalités
    print("\n=== Test des nouvelles fonctionnalités ===")
    
    # Ajouter un future
    db.add_future_asset('DAX', 'DAX', 'DAX Index', 'DAX', currency='EUR', source='TEST', contract_size=25)
    
    # Récupérer les futures
    futures = db.get_futures_for_underlying('DAX')
    print(f"Futures pour DAX: {futures}")
    
    # Récupérer le mapping
    mapping = db.get_futures_mapping()
    print(f"Mapping futures: {mapping}")
    
    # Résumé
    summary = db.get_summary()
    print(f"Résumé: {summary}")
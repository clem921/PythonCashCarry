"""
Fichier de configuration pour le programme Cash and Carry IBKR
"""

# Configuration IBKR
IBKR_CONFIG = {
    'host': '127.0.0.1',
    'port': 7497,  # Port par défaut pour TWS
    'client_id': 2,
    'account_id': '',  # Votre account ID IBKR (ex: 'DU1234567')
    'tws_path': r'C:\TWS API',  # Chemin vers TWS API (adapter selon votre installation)
    'paper_trading': True  # Mode papier (demo)
}

# Type de données de marché (1: Live, 2: Frozen, 3: Delayed, 4: Delayed Frozen)
MARKET_DATA_TYPE = 3

# === EXECUTION CONTROLS ===
EXECUTE_ORDERS = False  # Set to True to actually place orders. False = Simulation/Dry Run.
MAX_TRADES_PER_SESSION = 1 # Only take the BEST opportunity per session.

# Configuration des marchés et actifs
# Structure: Région -> Paramètres et Liste d'actifs
MARKETS = {
    'US': {
        'enabled': True,
        'stock_exchange': 'SMART',
        'future_exchange': '',
        'currency': 'USD',
    },
    'EUROZONE': {
        'enabled': True,
        'stock_exchange': 'SMART',
        'future_exchange': '',
        'currency': 'EUR',
    },
    'UK': {
        'enabled': False,
        'stock_exchange': 'SMART',
        'future_exchange': '',
        'currency': 'GBP',
    },
    'SWISS': {
        'enabled': False,
        'stock_exchange': 'SMART',
        'future_exchange': '',
        'currency': 'CHF',
    }
}

# Configuration de la stratégie
STRATEGY_CONFIG = {
    'min_spread_ratio': 1.05,  # Ratio minimum pour considérer une opportunité (5% au-dessus du théorique)
    'max_position_size': 10000,  # Taille maximale de position en devise locale
    'max_days_to_expiry': 180,  # Jours maximum avant échéance pour ouvrir (6 mois)
    'min_days_to_close': 2,  # Jours minimum avant échéance pour fermer
}

# Coûts de portage — valeurs par défaut si IBKR ne fournit pas de what-if
# Barème IBKR tiered EUR pour comptes < 100k€ (à ajuster selon votre profil)
COST_CONFIG = {
    'funding_rate': 0.0483,       # Taux de financement annuel (BM + spread IBKR, ~ESTR + 1.5%)
    'stock_commission_per_share': 0.0,  # Rempli par what-if IBKR si disponible
    'stock_commission_pct': 0.0005,     # Fallback: 0.05% de la valeur (barème IBKR tiered EUR)
    'stock_commission_min': 1.25,       # Commission minimum par ordre action (EUR, tiered)
    'future_commission_per_contract': 1.50,  # Commission par contrat future (EUR, tiered Euronext)
    'future_exchange_fee': 0.30,        # Frais d'exchange + clearing par contrat (EUR)
    'dividend_yield_default': 0.02,     # Rendement dividende moyen estimé (2% — fallback)
    'slippage_bps': 5,                  # Slippage estimé en bps (0.05%) par jambe
    'ftt_rate_fr': 0.003,              # Taxe sur transactions financières France (0.3%)
    'ftt_rate_it': 0.001,              # TTF Italie (0.1%)
}

# Configuration de la base de données
DB_CONFIG = {
    'name': 'cash_carry_positions.db',
    'table_name': 'positions'
}

# Configuration des logs
LOG_CONFIG = {
    'level': 'INFO',
    'file': 'cash_carry_trader.log',
    'max_size': 1024 * 1024 * 5,  # 5 MB
    'backup_count': 3
}

# Configuration des notifications
NOTIFICATION_CONFIG = {
    'email_enabled': False,
    'email_to': 'trader@example.com',
    'sms_enabled': False,
    'sms_to': '+1234567890'
}

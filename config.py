"""
Fichier de configuration pour le programme Cash and Carry IBKR
"""

# Configuration IBKR
IBKR_CONFIG = {
    'host': '127.0.0.1',
    'port': 7497,  # Port par défaut pour TWS
    'client_id': 2,
    'account_id': 'DU7117267',  # Compte démo
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
        'currency': 'USD',
    },
    'EUROZONE': {
        'enabled': True,
        'currency': 'EUR',
    },
    'UK': {
        'enabled': False,
        'currency': 'GBP',
    },
    'SWISS': {
        'enabled': False,
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

# Configuration de la stratégie Dividend Capture (achat action + vente future le jour ex-dividende)
DIVIDEND_CAPTURE_CONFIG = {
    'min_profit_pct': 0.1,           # Rentabilité nette minimum (% du nominal investi) pour considérer l'opportunité
    'max_position_size': 10000,      # Taille maximale de position en devise locale
    'preferred_future_expiry_days': 30,  # Préférer les futures expirant dans ~30 jours (court terme)
    'max_future_expiry_days': 180,   # Expiration max du future considéré
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

# Mapping exchange primaire IBKR -> devise native de l'action
# Utilisé pour créer les contrats actions cross-currency (ex: ULVR coté au LSE en GBP)
EXCHANGE_CURRENCY = {
    'SBF': 'EUR',     # Paris
    'AEB': 'EUR',     # Amsterdam
    'BVB': 'EUR',     # Brussels
    'EBR': 'EUR',     # Brussels (variante)
    'IBIS': 'EUR',    # Xetra / Francfort
    'BVME': 'EUR',    # Borsa Italiana (Milan)
    'BM': 'EUR',      # Bolsa de Madrid
    'ENXL': 'EUR',    # Euronext Lisbon
    'HEX': 'EUR',     # Helsinki (Nasdaq Nordic)
    'LSE': 'GBP',     # London Stock Exchange
    'OSE': 'NOK',     # Oslo
    'CSE': 'DKK',     # Copenhagen (Nasdaq Nordic)
    'SFB': 'SEK',     # Stockholm (Nasdaq Nordic)
    'EBS': 'CHF',     # SIX Swiss Exchange
    'VSE': 'EUR',     # Vienna
    'ISE': 'EUR',     # Irish Stock Exchange (Dublin)
}

# Configuration de la base de données
DB_CONFIG = {
    'name': 'cash_carry_positions.db',
    'table_name': 'positions'
}


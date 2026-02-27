# AGENTS.md — PythonCashCarry

## Commands
- **Run**: `python main.py` (requires TWS running on port 7497)
- **Test all**: `python -m pytest test/ -v`
- **Test single**: `python -m pytest test/test_strategy.py::TestCashCarryStrategy::test_name -v`
- **Update universe**: `python update_universe.py`
- **Activer le venv**: `.\venv\Scripts\activate` (Windows) — Toujours activer l'environnement virtuel avant d'exécuter des commandes Python.
- **Install deps**: `pip install -r requirements.txt` (inside `venv`)

## Architecture
- **main.py**: Core trading engine — `IBapi` (IBKR native API wrapper) and `CashCarryTrader` (strategy logic, order placement, opportunity scanning).
- **config.py**: All configuration — IBKR connection, market regions (US/EU/UK/Swiss), strategy params, DB settings.
- **universe_manager.py**: `UniverseDatabase` class — manages tradable asset universe in `universe.db` (SQLite, table `managed_assets`).
- **update_universe.py**: Populates `universe.db` from Euronext API or CSV files in `inputs/`.
- **test/**: Répertoire dédié à tous les fichiers de test. Tout fichier de test doit être créé dans ce répertoire et supprimé après utilisation (ne pas laisser de fichiers de test temporaires dans le projet).
- **test/test_strategy.py**: Unit tests (unittest) with mocked ibapi modules.
- **Databases**: `cash_carry_positions.db` (positions/trades), `universe.db` (asset universe). Both SQLite.
- **Utility scripts**: `extract_links.py`, `find_json.py`, `probe_urls.py` — Euronext URL/API discovery helpers.

## Code Style
- **Language**: Python 3.10+. Comments and prints in French.
- **Imports**: stdlib first, then third-party (`pandas`, `numpy`, `ibapi`), then local (`config`, `universe_manager`).
- **IBKR API**: Uses native `ibapi` (not `ib_insync`) via `sys.path.append(r'C:/TWS API/source/pythonclient')`.
- **DB access**: Raw `sqlite3` with context managers (`with sqlite3.connect(...) as conn`).
- **Error handling**: try/except with `print()` for logging; bare `except` used in some places.
- **Naming**: snake_case for functions/variables, PascalCase for classes. Config constants are UPPER_SNAKE_CASE dicts.

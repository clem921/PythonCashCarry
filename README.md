# Programme de Trading Cash and Carry pour IBKR

Ce programme implémente une stratégie de trading Cash and Carry pour Interactive Brokers (IBKR) en utilisant l'API TWS.

## Description

La stratégie Cash and Carry consiste à acheter un actif sous-jacent et vendre simultanément un contrat à terme sur le même actif, profitant ainsi de l'écart entre le prix au comptant et le prix à terme.

## Prérequis

- Compte IBKR (demo ou réel)
- TWS (Trader Workstation) installé et configuré
- API TWS activée
- Python 3.10 ou supérieur

## Installation

1. **Cloner le dépôt** (ou télécharger les fichiers):
   ```bash
   git clone https://github.com/votre-utilisateur/ibkr-cash-carry.git
   cd ibkr-cash-carry
   ```

2. **Créer un environnement virtuel**:
   ```bash
   python -m venv venv
   ```

3. **Activer l'environnement virtuel**:
   - Windows:
     ```bash
     .\venv\Scripts\activate
     ```
   - macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. **Installer les dépendances**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Configurer TWS**:
   - Lancer TWS
   - Aller dans `Edit` > `Global Configuration` > `API` > `Settings`
   - Cocher "Enable ActiveX and Socket Clients"
   - Définir le port socket (par défaut: 7497)
   - Sauvegarder et redémarrer TWS

2. **Configurer le programme**:
   - Modifier les variables en haut de `main.py` selon vos besoins
   - Définir les paramètres de connexion IBKR
   - Configurer la stratégie (seuils, tailles de position, etc.)
   - Choisir le marché/region (US, EU, HK, JP)

**Note**: Le programme utilise maintenant une détection dynamique des instruments disponibles sur IBKR. Les listes statiques de symboles ont été supprimées du fichier de configuration car elles ne sont plus nécessaires.

## Configuration des marchés/bourses

Le programme supporte maintenant la configuration flexible des marchés et bourses pour le trading :

### Configuration par défaut

```python
MARKET_CONFIG = {
    'stocks': {
        'US': 'SMART',      # Actions US
        'EU': 'IBIS',       # Actions Europe
        'HK': 'SEHK',      # Actions Hong Kong
        'JP': 'JAPAN',      # Actions Japon
    },
    'futures': {
        'US': 'GLOBEX',     # Futures US (CME)
        'EU': 'EUREX',      # Futures Europe
        'HK': 'HKFE',       # Futures Hong Kong
        'JP': 'OSE',        # Futures Japon
    },
    'options': {
        'US': 'SMART',      # Options US
        'EU': 'IBIS',       # Options Europe
    }
}

# Configuration actuelle
CURRENT_MARKET = 'US'  # US, EU, HK, ou JP
```

### Changer de marché dynamiquement

Le programme inclut une méthode `set_market()` pour changer de région de marché :

```python
# Exemple d'utilisation
trader = CashCarryTrader()
trader.set_market('EU')  # Changer vers le marché européen
```

### Régions de marché supportées

- **US**: États-Unis (SMART pour actions, GLOBEX pour futures)
- **EU**: Europe (IBIS pour actions, EUREX pour futures)
- **HK**: Hong Kong (SEHK pour actions, HKFE pour futures)
- **JP**: Japon (JAPAN pour actions, OSE pour futures)

### Avantages

- **Flexibilité**: Changez de marché sans modifier le code
- **Diversification**: Tradez sur différents marchés avec la même stratégie
- **Adaptabilité**: Accédez à différents instruments selon la région
- **Simplicité**: Configuration centralisée et facile à modifier

## Exemple complet

```python
# Créer le trader
trader = CashCarryTrader()

# Changer de marché vers l'Europe
trader.set_market('EU')

# Se connecter et trader
trader.connect_to_ibkr()
trader.find_cash_carry_opportunities()

# Changer vers le Japon
trader.set_market('JP')

# Continuer le trading
trader.find_cash_carry_opportunities()
```

Cette fonctionnalité permet d'adapter facilement la stratégie Cash and Carry à différents marchés et bourses selon vos besoins de trading.

## Modes de fonctionnement

Le programme supporte maintenant deux modes de fonctionnement :

### Mode Différé (Demo) - Par défaut
- `USE_REAL_TIME_DATA = False`
- Compte: `DU7117267` (compte démo IBKR)
- Données de marché: Différées (gratuites)
- Idéal pour les tests et le développement

### Mode Temps Réel
- `USE_REAL_TIME_DATA = True`
- Compte: `U1234567` (à remplacer par votre vrai compte)
- Données de marché: Temps réel (nécessite une souscription)
- Pour le trading en production

## Gestion améliorée des contrats à terme

Le programme inclut maintenant une gestion améliorée des contrats à terme :

- **Récupération dynamique**: Les contrats à terme disponibles sont récupérés automatiquement depuis l'API IBKR
- **Échéances standardisées**: Définies dans le dictionnaire `FUTURES_EXPIRIES` (utilisées comme fallback)
- **Gestion des symboles locaux**: Pour les contrats standard comme ES, NQ
- **Gestion automatique des échéances**: Utilisation des échéances par défaut si non spécifiées

## Récupération dynamique des contrats à terme

La méthode `fetch_available_futures()` récupère automatiquement les contrats à terme disponibles depuis IBKR :

```python
def fetch_available_futures(self):
    """Récupérer dynamiquement les contrats à terme disponibles depuis IBKR"""
    print("Récupération des contrats à terme disponibles...")

    # Liste des symboles de contrats à terme à rechercher
    future_symbols = ['ES', 'NQ', 'YM', 'RTY', 'GC', 'SI', 'CL', 'NG', 'ZB', 'ZN']

    for symbol in future_symbols:
        try:
            # Créer un contrat générique pour ce symbole
            contract = Contract()
            contract.symbol = symbol
            contract.secType = 'FUT'
            contract.exchange = 'GLOBEX'

            # Demander les détails du contrat
            req_id = 1000 + future_symbols.index(symbol)
            self.ib.reqContractDetails(req_id, contract)

            # Stocker les contrats disponibles
            if req_id in self.ib.contract_details:
                for contract_detail in self.ib.contract_details[req_id]:
                    contract_key = contract_detail.contract.symbol
                    AVAILABLE_FUTURES[contract_key].append(contract_detail.contract)
                    print(f"Trouvé contrat à terme: {contract_key} - {contract_detail.contract.lastTradeDateOrContractMonth}")

        except Exception as e:
            print(f"Impossible de récupérer les détails pour {symbol}: {e}")
```

## Avantages de la récupération dynamique

- **Précision**: Utilise les contrats réels disponibles sur la plateforme
- **Adaptabilité**: S'adapte aux contrats disponibles sans modification du code
- **Transparence**: Affiche les contrats trouvés avec leurs échéances
- **Résilience**: Utilise des valeurs par défaut en cas d'erreur de récupération

## Liste des contrats à terme recherchés

Le programme recherche automatiquement les contrats suivants :
- **Indices**: ES (S&P 500), NQ (Nasdaq 100), YM (Dow Jones), RTY (Russell 2000)
- **Matières premières**: GC (Or), SI (Argent), CL (Pétrole), NG (Gaz naturel)
- **Obligations**: ZB (T-Bond), ZN (T-Note)

Ces contrats couvrent les principaux marchés et permettent une stratégie Cash and Carry diversifiée.

## Configuration des échéances

Les échéances des contrats à terme sont configurables dans le dictionnaire `FUTURES_EXPIRIES` :
```python
FUTURES_EXPIRIES = {
    'AAPL': '202406',  # Juin 2024
    'MSFT': '202406',  # Juin 2024
    'SPY': '202406',   # Juin 2024
    'QQQ': '202406',   # Juin 2024
    'ES': '202406',    # S&P 500 e-mini
    'NQ': '202406',    # Nasdaq 100 e-mini
}
```

## Basculer entre les modes

Pour basculer entre les modes, modifiez simplement la variable `USE_REAL_TIME_DATA` dans `main.py` :
```python
USE_REAL_TIME_DATA = False  # Pour le mode différé
# USE_REAL_TIME_DATA = True  # Pour le mode temps réel
```

## Configuration TWS pour les données API

Pour éviter les erreurs de souscription (erreur 10089) :
1. Dans TWS, allez dans `Edit` > `Global Configuration` > `API` > `Settings`
2. Cliquez sur "Market Data Permissions"
3. Autorisez les connexions API pour les données de marché
4. Sauvegardez et redémarrez TWS

Pour les contrats à terme, assurez-vous que les échéances sont valides et que les symboles locaux sont correctement configurés.

## Utilisation

1. **Lancer TWS** et s'assurer qu'il est connecté

2. **Exécuter le programme**:
   ```bash
   python main.py
   ```

3. **Options avancées**:
   - Pour exécuter en mode continu (surveillance permanente):
     ```bash
     python main.py --continuous
     ```
   - Pour spécifier un fichier de configuration personnalisé:
     ```bash
     python main.py --config mon_config.py
     ```

## Structure du projet

- `main.py`: Programme principal
- `config.py`: Fichier de configuration
- `requirements.txt`: Dépendances Python
- `cash_carry_positions.db`: Base de données SQLite (créée automatiquement)

## Stratégie

1. **Détection dynamique des opportunités**:
   - Le programme récupère automatiquement les instruments disponibles depuis IBKR
   - Il analyse les écarts entre les prix au comptant et les prix à terme pour tous les contrats disponibles
   - Il calcule l'écart théorique basé sur les taux d'intérêt
   - Une opportunité est identifiée lorsque l'écart réel est supérieur à l'écart théorique

2. **Exécution des transactions**:
   - Achat de l'actif sous-jacent
   - Vente simultanée du contrat à terme correspondant
   - Enregistrement de la position dans la base de données avec les contrats dynamiques

3. **Gestion des positions**:
   - Surveillance continue des positions ouvertes
   - Fermeture automatique à l'approche de l'échéance
   - Fermeture en cas de conditions de sortie atteintes
   - Utilisation des contrats dynamiques pour la fermeture des positions

## Fonctionnalités avancées

- **Détection automatique des instruments**: Le programme découvre dynamiquement les actions et contrats à terme disponibles sur IBKR
- **Appariement intelligent**: Il trouve automatiquement les contrats à terme correspondants pour chaque action
- **Gestion dynamique des contrats**: Toutes les opérations utilisent les contrats réels disponibles sur la plateforme
- **Résilience**: En cas d'erreur, le programme utilise une liste de secours pour continuer à fonctionner

## Gestion complète des coûts

Le programme prend en compte tous les coûts importants pour une évaluation réaliste de la rentabilité :

1. **Coûts de portage**: Calcul basé sur les taux sans risque et les coûts de financement
2. **Frais de transaction**: Récupérés dynamiquement depuis l'API IBKR
3. **Calcul de l'écart théorique**: Intègre tous les coûts pour déterminer le seuil de rentabilité
4. **Profit net**: Calcul précis du P&L en tenant compte de tous les frais

## Récupération dynamique des frais

**Nouvelle fonctionnalité**: Le programme récupère maintenant automatiquement les taux de commission et frais directement depuis l'API IBKR :

- **Taux de commission sur actions**: Récupéré depuis `StockCommissionRate`
- **Taux de commission sur futures**: Récupéré depuis `FutureCommissionRate`
- **Taux de financement**: Récupéré depuis `FinancingRate`
- **Commission minimale**: Récupérée depuis `MinCommission`
- **Commission maximale**: Récupérée depuis `MaxCommission`

## Avantages

- **Précision**: Utilise les taux réels de votre compte IBKR
- **Adaptabilité**: S'adapte automatiquement aux changements de structure de frais
- **Transparence**: Affiche les taux récupérés et les calculs détaillés
- **Résilience**: Utilise des valeurs par défaut en cas d'erreur de récupération

## Paramètres de coût

Les paramètres sont maintenant récupérés dynamiquement, mais voici les valeurs par défaut utilisées :
- Taux sans risque: 2% annuel (peut être récupéré depuis une API externe)
- Coût de financement: 1.5% annuel (dynamique)
- Frais de transaction: 0.05% par transaction (dynamique)
- Temps jusqu'à échéance: 3 mois (ajustable)

Le programme affiche les taux récupérés et les calculs détaillés pour une transparence totale.

## Base de données

Le programme utilise SQLite pour enregistrer:
- Les positions ouvertes et fermées
- Les prix d'entrée et de sortie
- Les dates et heures des transactions
- Les profits et pertes

## Sécurité

- **Mode papier**: Le programme est configuré pour utiliser le compte démo par défaut
- **Gestion des risques**: Limites de position et stops de protection intégrés
- **Journalisation**: Toutes les opérations sont enregistrées

## Dépendances

- `ib_insync`: Bibliothèque pour l'API IBKR
- `pandas`: Manipulation de données
- `numpy`: Calculs numériques
- `sqlite3`: Base de données intégrée

## Avertissements

- Ce programme est à des fins éducatives et de démonstration
- Ne pas utiliser avec un compte réel sans tests approfondis
- Le trading comporte des risques de perte financière
- L'auteur n'est pas responsable des pertes éventuelles

## License

MIT License - Voir le fichier LICENSE pour plus de détails.

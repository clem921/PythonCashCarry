"""Diagnostic des actifs code=300 : verifier underlying_primary_exchange vs primary_exchange"""
import sqlite3

conn = sqlite3.connect('universe.db')
c = conn.cursor()

# Symboles qui ont echoue avec code=300
failing_underlyings = [
    'KBC', 'KRZ', 'PROX', 'UCB', 'UMI', 'BEKB',  # EBR / Brussels
    'NOS', 'RENE', 'SON', 'NVG',                    # ENXL / Lisbon
    'STLA', 'SGBAF', 'RYA', 'SAN',                  # DPAR/DAMS/DMIL
    'NDA', 'NOVOB', 'NSISB', 'MTGB',                # Nordic
    'ONCIN', 'KINVB', 'SECUB', 'VOLCARB',            # Nordic composites
    'TRELB', 'TRYG', 'SCAB', 'SHBA', 'SWEDA',       # Nordic SFB/CSE
]

print(f"{'symbol':<8} {'under_sym':<12} {'under_prim_exch':<16} {'prim_exch':<12} {'deriv_exch':<12} {'under_isin':<14} {'source':<20}")
print("-" * 110)

for und in failing_underlyings:
    c.execute("""
        SELECT symbol, underlying_symbol, underlying_primary_exchange, primary_exchange, 
               derivatives_exchange, underlying_isin, source
        FROM managed_assets 
        WHERE underlying_symbol = ? AND active = 1
        LIMIT 1
    """, (und,))
    row = c.fetchone()
    if row:
        print(f"{row[0]:<8} {row[1]:<12} {row[2] or '(vide)':<16} {row[3] or '(vide)':<12} {row[4] or '(vide)':<12} {row[5] or '(vide)':<14} {row[6]:<20}")
    else:
        print(f"{'?':<8} {und:<12} {'NON TROUVE'}")

# Stats: combien d'actifs ont un underlying_primary_exchange vide ?
c.execute("""
    SELECT COUNT(*) FROM managed_assets 
    WHERE active=1 AND product_type='FUT' 
    AND (underlying_primary_exchange IS NULL OR underlying_primary_exchange = '')
""")
empty_count = c.fetchone()[0]

c.execute("SELECT COUNT(*) FROM managed_assets WHERE active=1 AND product_type='FUT'")
total_fut = c.fetchone()[0]

print(f"\n=== Stats ===")
print(f"Futures avec underlying_primary_exchange vide : {empty_count}/{total_fut}")

# Voir les exchanges utilises
c.execute("""
    SELECT underlying_primary_exchange, COUNT(*) 
    FROM managed_assets WHERE active=1 AND product_type='FUT'
    GROUP BY underlying_primary_exchange ORDER BY COUNT(*) DESC
""")
print(f"\nDistribution underlying_primary_exchange:")
for row in c.fetchall():
    print(f"  {row[0] or '(vide)':<16} {row[1]}")

conn.close()

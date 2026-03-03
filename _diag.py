import sqlite3
conn = sqlite3.connect('universe.db')
c = conn.cursor()

# Check SS ISINs
c.execute("""SELECT symbol, underlying_symbol, underlying_isin, derivatives_exchange, currency
    FROM managed_assets WHERE active=1 AND product_type='FUT' AND underlying_isin LIKE 'SS%' LIMIT 10""")
print("Examples of SS ISINs:")
for r in c.fetchall():
    print(f"  {r}")

# Check QS ISINs
c.execute("""SELECT symbol, underlying_symbol, underlying_isin, derivatives_exchange, currency
    FROM managed_assets WHERE active=1 AND product_type='FUT' AND underlying_isin LIKE 'QS%'""")
print("\nQS ISINs:")
for r in c.fetchall():
    print(f"  {r}")

# Check NL with empty exchange
c.execute("""SELECT symbol, underlying_symbol, underlying_isin, derivatives_exchange, currency
    FROM managed_assets WHERE active=1 AND product_type='FUT' AND underlying_isin LIKE 'NL%'
    AND (underlying_primary_exchange IS NULL OR underlying_primary_exchange = '') LIMIT 5""")
print("\nNL ISINs with empty underlying_primary_exchange:")
for r in c.fetchall():
    print(f"  {r}")

# Check which scraper is populating SS ISINs
c.execute("""SELECT source, COUNT(*) FROM managed_assets 
    WHERE active=1 AND product_type='FUT' AND underlying_isin LIKE 'SS%'
    GROUP BY source""")
print("\nSources for SS ISINs:")
for r in c.fetchall():
    print(f"  {r}")

# Check if the ISIN fallback is being applied at all
c.execute("""SELECT source, COUNT(*) FROM managed_assets 
    WHERE active=1 AND product_type='FUT'
    AND (underlying_primary_exchange IS NULL OR underlying_primary_exchange = '')
    GROUP BY source""")
print("\nSources for empty underlying_primary_exchange:")
for r in c.fetchall():
    print(f"  {r}")

conn.close()

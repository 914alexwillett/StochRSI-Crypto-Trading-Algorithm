import sqlite3

# desired filepath for database
path = r'filepath\for\trade_database.db'

# setup connection to the sqlite3 Database API
# for the trade database about to be created
connection = sqlite3.connect(path)
cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS asset_prices (
        id INTEGER,
        asset_id INTEGER,
        datetime NOT NULL,
        close NOT NULL,
        PRIMARY KEY(id),
        UNIQUE(datetime, asset_id),
        FOREIGN KEY(asset_id) REFERENCES assets(id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        id INTEGER PRIMARY KEY,
        symbol NOT NULL,
        name NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS buy_log (
        id INTEGER PRIMARY KEY,
        datetime NOT NULL,
        cost REAL NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        asset_id INTEGER NOT NULL,
        FOREIGN KEY (asset_id) REFERENCES assets (id),
        UNIQUE (datetime, price, asset_id)
)
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS portfolio (
        id INTEGER,
        asset_id INTEGER NOT NULL,
        asset_name TEXT NOT NULL,
        trading BOOLEAN NOT NULL CHECK(trading IN (0, 1)),
        quantity REAL NOT NULL,
        FOREIGN KEY(asset_id) REFERENCES assets(id),
        PRIMARY KEY(id)
)
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS sell_log (
        id INTEGER NOT NULL,
        datetime TEXT NOT NULL,
        price REAL NOT NULL,
        quantity REAL NOT NULL,
        profit REAL NOT NULL,
        profit_ratio REAL NOT NULL,
        avg_cost REAL NOT NULL,
        asset_id INTEGER NOT NULL,
        PRIMARY KEY(id),
        UNIQUE(datetime, asset_id),
        FOREIGN KEY(asset_id) REFERENCES assets(id)
)
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS trade_status (
        id INTEGER PRIMARY KEY,
        buy_position BOOLEAN NOT NULL CHECK (buy_position IN (0,1)),
        sell_position BOOLEAN NOT NULL CHECK (sell_position IN (0,1)),
        asset_id INTEGER NOT NULL,
        FOREIGN KEY (asset_id) REFERENCES assets (id)
)
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS transaction_history (
        transaction_id INTEGER,
        datetime TEXT NOT NULL,
        price REAL NOT NULL,
        quantity REAL NOT NULL,
        fee REAL NOT NULL,
        buy_sell TEXT NOT NULL,
        asset_id INTEGER NOT NULL,
        asset_name TEXT NOT NULL,
        FOREIGN KEY(asset_id) REFERENCES assets(id),
        UNIQUE(datetime, asset_id, buy_sell, price, quantity),
        PRIMARY KEY(transaction_id)
)
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS transaction_history_missed (
        transaction_id INTEGER,
        datetime TEXT NOT NULL,
        price REAL NOT NULL,
        quantity REAL NOT NULL,
        fee REAL NOT NULL,
        buy_sell TEXT NOT NULL,
        asset_id INTEGER NOT NULL,
        asset_name TEXT NOT NULL,
        FOREIGN KEY(asset_id) REFERENCES assets(id),
        UNIQUE(datetime, asset_id, buy_sell, price, quantity),
        PRIMARY KEY(transaction_id)
)
""")

# Populates "assets" table with a unique asset id, symbol and type 
# asset_id useful for linking entries with multiple tables and
# differentiating trade logic actions regarding the various assets

assets = [
    (1, 'BTC', 'Bitcoin'),
    (2, 'ETH', 'Ethereum'),
    (3, '$USD', 'Cash')]

for row in assets:
    cursor.execute("""
        INSERT OR IGNORE INTO assets (id, symbol, name)
        VALUES (?, ?, ?) """, 
            (row[0], row[1], row[2]))

# Populates "portfolio" table with unique id for each row in the table,
# the asset_id related to the underlying coin it is related to.
# Splits each coin (BTC and ETH) into 3 portions of the portfolio;
# a part of each coin's position, that is for trading and then
# a part that is allocated for holding indefinately.
# Finally, the cash balance split into a portion used exclusively for each coin

portfolio_default = [
    (1, 1, 'BTC_trade', 1, 0.0),
    (2, 1, 'BTC_hold', 0, 0.0),
    (3, 2, 'ETH_trade', 1, 0.0),
    (4, 2, 'ETH_hold', 0, 0.0),
    (5, 1, 'Cash (BTC)', 1, 300),
    (6, 2, 'Cash (ETH)', 1, 300)]

for row in portfolio_default:
    cursor.execute("""
        INSERT OR IGNORE INTO portfolio (id, asset_id, asset_name, trading, quantity)
        VALUES (?, ?, ?, ?, ?)""",
            (row[0], row[1], row[2], row[3], row[4]))

    
connection.commit()
cursor.close()
print("Trade database created")
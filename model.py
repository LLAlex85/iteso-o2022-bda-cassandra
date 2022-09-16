#!/usr/bin/env python3
import logging

from datetime import datetime
import random

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DECIMAL,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    ) WITH CLUSTERING ORDER BY (symbol ASC)
"""

SELECT_POSITION_BY_ACCOUNTS = """
    SELECT symbol, quantity
    FROM positions_by_account
    WHERE account = ?
"""

def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)

def get_positions(session):
    account = input("Enter your account: ")
    log.info(f"Retrieving positions in {account}")
    stmt = session.prepare(SELECT_POSITION_BY_ACCOUNTS)
    rows = session.execute(stmt, [account])
    print(f"Positions of account {account}: ")
    for row in rows:
        print(f"=== Symbol: {row.symbol} ===")
        print(f"- Quantity: {row.quantity}")
        print(f"- Balance: ${ round(float(row.quantity) * get_instrument_value(row.symbol), 2)} ")

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""
SELECT_TRADES_BY_ACCOUNT = """
    SELECT toDate(trade_id), trade_id, type, symbol, shares, price, amount
    FROM trades_by_a_d
    WHERE account = ? and trade_id > maxTimeuuid( ? )  and trade_id < minTimeuuid( ? )
"""

#Q3.1 Find all trades for an account; order by trade date
def get_all_trades_by_account(session, table, query, args):
    log.info(f"Retrieving trades in {args[0]}")
    stmt = session.prepare(query.create_query(table, ["toDate(trade_id)", "trade_id", "type","symbol", "shares", "price", "amount"], "account"))
    rows = session.execute(stmt, args)
    print(f"Trades of account {args[0]}: ")
    for row in rows:
        print(f"=== Trade ID: {row.trade_id} ===")
        print(f"- Date: {row.system_todate_trade_id}")
        print(f"- Quantity: {row.type}")
        print(f"- Symbol: {row.symbol}")
        print(f"- Shares: {row.shares}")
        print(f"- Price: {row.price}")
        print(f"- Amount: {row.amount}")

SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""

CREATE_TRADES_BY_ACCOUNT_TRANSACTION_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, trade_id )
    ) WITH CLUSTERING ORDER BY (type DESC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_TRANSACTION_SYMBOL_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (type DESC, symbol DESC, trade_id DESC)
"""

CREATE_TRADES_BY_ACCOUNT_SYMBOL_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol DESC, trade_id DESC)
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TRANSACTION_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TRANSACTION_SYMBOL_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_SYMBOL_DATE_TABLE)

#Q1 Find info about all accounts of a user
def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        print(f"=== Account: {row.account_number} ===")
        print(f"- Cash Balance: {row.cash_balance}")


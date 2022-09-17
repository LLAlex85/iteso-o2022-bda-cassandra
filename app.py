#!/usr/bin/env python3
import logging
import os
import string
from datetime import datetime

from cassandra.cluster import Cluster

import model
import trade_table

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '172.17.0.2')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_trade_history_menu():
    thm_options = {
        1: "Transaction Type (Buy/Sell)",
        2: "Instrument Symbol"
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username

def get_symbol():
    return input('Introduce symbol: ')


def get_type():
    return input('Introduce type: ')

table = {
    0: ["trades_by_a_d", None, None],
    1: ["trades_by_a_td", get_type, trade_table.filter_by_type],
    2: ["trades_by_a_sd", get_symbol, trade_table.filter_by_symbol],
    3: ["trades_by_a_std", None, None],
}

def get_query_and_args(filter_options):

    options = []
    if (filter_options):
        options = filter_options.split(',')
        options = [int(x) for x in options]

    index = 0
    for i in options:
        index |= i << 0
    table_to_use = table[index][0]
    
    args = []
    #Create Concrete Query object
    query = trade_table.ConcreteQuery()

    args.append(input('Enter Account (Mandatory): '))

    for i in options:
        query = table[i][2](query)
        args.append(table[i][1]())
    
    ans = input("Filter By Date? [Yy/Nn]: ")
    if (ans.lower() == 'y'):
        start_date = datetime.strptime(input("Enter start Date (YYYY-MM-DD):"),'%Y-%m-%d')
        end_date = datetime.strptime(input("Enter End Date (YYYY-MM-DD):"),'%Y-%m-%d')
        args.append(start_date)
        args.append(end_date)
        query = trade_table.filter_by_date(query)

    return table_to_use, query, args


def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    username = set_username()

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.get_user_accounts(session, username)
        if option == 2:
            model.get_positions(session)
        if option == 3:
            print_trade_history_menu()
            tv_option = input('Enter your trade filter options separated by commas e.g. 1,2. Hit only enter to show all trades: ')
            table, query, args = get_query_and_args(tv_option)
            model.get_all_trades_by_account(session, table, query, args)

        if option == 4:
            username = set_username()
        if option == 5:
            exit(0)


if __name__ == '__main__':
    main()

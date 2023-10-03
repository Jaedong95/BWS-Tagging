import pandas as pd
import argparse
import pymysql
import pickle
import json
import csv
import os 
from attrdict import AttrDict
from database import MySQLDB

def main():
    global conn 
    global curs 
    global mysqldb
    
    with open(os.path.join(cli_argse.config_path, cli_argse.db_config)) as f:
        args = AttrDict(json.load(f))

    mysqldb = MySQLDB(args)
    mysqldb.connect()
    try:
        mysqldb.create_idx_tb()
    except:
        pass
    mysqldb.create_crieria_tb(cli_argse.criteria)
    mysqldb.create_set_tb(cli_argse.criteria)
    mysqldb.create_log_tb(cli_argse.criteria)
    mysqldb.cancel_connect()
    

if __name__ == '__main__':
    global cli_argse 
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config_path", type=str, default='config')
    cli_parser.add_argument("--db_config", type=str, default='db_config.json')
    cli_parser.add_argument("--criteria", type=int)
    cli_argse = cli_parser.parse_args()
    main()
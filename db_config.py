import cx_Oracle
import json

file = open("PATH_FOR_JSON_FILE")
conn_str = json.load(file)

lib_dir = "PATH_FOR_INSTANTCLIENT"
tsn = conn_str["str"]
user = conn_str["user"]
pwd = conn_str["pwd"]

encoding = "UTF-8"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

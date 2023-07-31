import db_config as db
import cx_Oracle
import pandas as pd


class oracle_conn(object):
    def __init__(self, user=db.user, pwd=db.pwd, tsn=db.tsn):
        self.user = user
        self.pwd = pwd
        self.tsn = tsn
        self.connector = None
        self.cursor = None

    def __enter__(self):
        self.connector = cx_Oracle.connect(
            user=self.user, password=self.pwd, dsn=self.tsn
        )
        self.cursor = self.connector.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
            self.cursor.close()
        else:
            self.connector.rollback()
        self.connector.close()


class oracle_sql(object):
    def __init__(self, user=db.user, pwd=db.pwd, tsn=db.tsn):
        self.user = user
        self.pwd = pwd
        self.tsn = tsn
        self.connector = None

    def __enter__(self):
        self.connector = cx_Oracle.connect(
            user=self.user, password=self.pwd, dsn=self.tsn
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()

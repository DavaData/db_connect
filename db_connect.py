"""
db_connect is a db connect tools to connect to common databases and execute common commands against database.
for example: run sql, stored procedure, insert data, etc.
"""
import oracledb
import pymysql
import psycopg2
from clickhouse_driver import Client

class MysqlConnector:

    db_type = 'Mysql'

    def __init__(self, dbconfig):
        self.mysql_conn = pymysql.connect(**dbconfig)
        self.mysql_cursor = self.mysql_conn.cursor()

    def query_data(self, sql):
        self.mysql_cursor.execute(sql)
        rows = list(self.mysql_cursor.fetchall())
        columns = [col[0] for col in self.mysql_cursor.description]  # 获取列名
        return rows, columns

    def insert_data(self, insert_query, rows):
        # 执行批量插入
        self.mysql_cursor.executemany(insert_query, rows)
        self.mysql_conn.commit()
        print(f"成功插入 {self.mysql_cursor.rowcount} 行数据")

    def execute_dml(self, sql):
        self.mysql_cursor.execute(sql)
        self.mysql_conn.commit()
        print(f"成功执行 {sql} 语句")

    def set_session_read_committed(self):
        self.mysql_cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    def close(self):
        self.mysql_conn.close()
        self.mysql_cursor.close()

class OracleConnector:

    db_type = 'Oracle'

    def __init__(self, dbconfig, ora_lib_dir=''):
        if ora_lib_dir != '':
            oracledb.init_oracle_client(ora_lib_dir)
            # 数据库连接配置
            dsn = oracledb.makedsn(dbconfig['host'], dbconfig['port'], service_name=dbconfig['service_name'])
        else:
            dsn = f"{dbconfig['host']}:{dbconfig['port']}/{dbconfig['service_name']}"  #10.106.96.202:1521/tfdw"
        # 连接到数据库
        self.oracle_conn = oracledb.connect(user=dbconfig['user'], password=dbconfig['password'], dsn=dsn)
        self.oracle_cursor = self.oracle_conn.cursor()

    def execute_dml(self, sql):
        self.oracle_cursor.execute(sql)
        self.oracle_conn.commit()
        print(f"成功执行 {sql} 语句")

    def execute_dml_from_file(self, file_name):
        with open(file_name, encoding='utf-8') as f:
            sql = f.read()  # str 类型
            print(sql)
            self.oracle_cursor.execute(sql)
            self.oracle_conn.commit()

    def insert_data(self, insert_query, data):
        # 执行批量插入
        self.oracle_cursor.executemany(insert_query, data)
        self.oracle_conn.commit()
        print(f"成功插入 {self.oracle_cursor.rowcount} 行数据")

    def query_data(self, sql):
        self.oracle_cursor.execute(sql)
        rows = self.oracle_cursor.fetchall()
        columns = [col[0] for col in self.oracle_cursor.description]  # 获取列名
        # print(columns)
        # 将结果转换为字典列表
        # result_dicts = [dict(zip(columns, row)) for row in rows]
        # print(result_dicts)
        return rows, columns

    def run_sp(self, sp_name, db_link=''):
        sp_sql = f"CALL {sp_name}{db_link}()"
        print(sp_sql)
        self.oracle_cursor.execute(sp_sql)

    def close(self):
        self.oracle_cursor.close()
        self.oracle_conn.close()

class PostgreSQLConnector:

    db_type = 'PostgreSQL'

    def __init__(self, dbconfig):
        self.pg_conn = psycopg2.connect(**dbconfig)
        self.pg_cursor = self.pg_conn.cursor()

    def execute_dml(self, sql):
        self.pg_cursor.execute(sql)
        self.pg_conn.commit()
        print(f"成功执行 {sql} 语句")

    def query_data(self, sql):
        self.pg_cursor.execute(sql)
        rows = self.pg_cursor.fetchall()
        columns = [col[0] for col in self.pg_cursor.description]  # 获取列名
        return rows, columns

    def close(self):
        self.pg_cursor.close()
        self.pg_conn.close()

class ClickHouseConnector:

    db_type = 'ClickHouse'

    def __init__(self, dbconfig):
        self.client = Client(**dbconfig)

    def query_data(self, sql):
        rows, column_types = self.client.execute(sql, with_column_types=True)
        columns = [col[0] for col in column_types]
        return rows, columns

    def close(self):
        self.client.disconnect()

class DataTransfer:

    def __init__(self, src_db, tgt_db):
        self.src_db = src_db
        self.tgt_db = tgt_db

    def truncate_and_insert(self, source_name, target_name):
        rows, columns = self.src_db.query_data(f"SELECT * FROM {source_name}")
        self.tgt_db.execute_dml(f"truncate table {target_name}")
        if self.tgt_db.db_type == 'Mysql':
            insert_columns_foramt = "{}".format(", ".join(["%s"] * len(columns)))
        elif self.tgt_db.db_type == 'Oracle':
            columns_list = [f":{column}" for column in columns]
            # columns_list = [f":{str(i + 1)}" for i in range(len(columns))]
            insert_columns_foramt = "{}".format(", ".join(columns_list))
        insert_query = f"INSERT INTO {target_name} VALUES ({insert_columns_foramt})"
        print(insert_query)

        self.tgt_db.insert_data(insert_query, rows)

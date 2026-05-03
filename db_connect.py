"""
db_connect is a db connect tools to connect to common databases and execute common commands against database.
for example: run sql, stored procedure, insert data, data exploration, etc.
"""
import oracledb
import pymysql
import dmPython
import sqlite3
import duckdb
import psycopg2
from clickhouse_driver import Client
from datetime import datetime, date

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

    def table_exploration(self, schema_name, cal_row_cnt_flag=False):
        """获取某个schema下面的所有表的：表名，表注释，表行数"""
        sql = f"""SELECT TABLE_NAME, TABLE_COMMENT, TABLE_ROWS 
                    FROM INFORMATION_SCHEMA.TABLES T
                    WHERE TABLE_SCHEMA = '{schema_name}'
              """
        rows, columns = self.query_data(sql)

        new_rows = []
        for row in rows:
            if cal_row_cnt_flag:
                cnt_value, cnt_column = self.query_data(f"SELECT COUNT(*) AS CNT FROM {schema_name}.{row[0]}")
                row_cnt = cnt_value[0][0]
            else:
                row_cnt = row[2]
            new_rows.append([row[0], row[1], row_cnt])
        return new_rows, columns

    def get_table_column_info(self, schema_name, table_name):
        # 获取字段信息和字段注释
        rows, columns = self.query_data(f"SELECT COLUMN_NAME, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'")
        column_name = [col[0] for col in rows]
        column_comment = [col[1] for col in rows]
        return column_name, column_comment

    def get_table_sample_data(self, schema_name, table_name, sample_size=50):
        """获取表的前***行示例数据"""
        rows, columns = self.query_data(f"SELECT * FROM {schema_name}.{table_name} LIMIT {sample_size}")
        new_rows = []
        for row in rows:
            row_convert = []
            for value in list(row):
                if isinstance(value, datetime):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                if isinstance(value, date):
                    value = value.strftime('%Y-%m-%d')
                if isinstance(value, bytes):
                    value = value.decode('utf-8', errors='ignore')
                row_convert.append(value)
            new_rows.append(row_convert)
        return new_rows

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

    def table_exploration(self, schema_name, cal_row_cnt_flag=False):
        """获取某个schema下面的所有表的：表名，表注释，表行数"""
        sql = f"""SELECT T1.TABLE_NAME, T2.COMMENTS, T1.NUM_ROWS
                    FROM ALL_TABLES T1
                    JOIN ALL_TAB_COMMENTS T2
                    ON T1.OWNER = T2.OWNER
                    AND T1.TABLE_NAME = T2.TABLE_NAME
                    AND T2.TABLE_TYPE = 'TABLE'
                    WHERE T1.OWNER = UPPER('{schema_name}')
                    ORDER BY T1.TABLE_NAME
              """
        rows, columns = self.query_data(sql)

        new_rows = []
        for row in rows:
            if cal_row_cnt_flag:
                cnt_value, cnt_column = self.query_data(f"SELECT COUNT(*) AS CNT FROM {schema_name}.{row[0]}")
                row_cnt = cnt_value[0][0]
            else:
                row_cnt = row[2]
            new_rows.append([row[0], row[1], row_cnt])
        return new_rows, columns

    def get_table_column_info(self, schema_name, table_name):
        # 获取字段信息和字段注释
        rows, columns = self.query_data(f"SELECT COLUMN_NAME, COMMENTS FROM ALL_COL_COMMENTS WHERE OWNER = UPPER('{schema_name}') AND TABLE_NAME = UPPER('{table_name}')")
        column_name = [col[0] for col in rows]
        column_comment = [col[1] for col in rows]
        return column_name, column_comment

    def get_table_sample_data(self, schema_name, table_name, sample_size=50):
        """获取表的前***行示例数据"""
        rows, columns = self.query_data(f"SELECT * FROM {schema_name}.{table_name} WHERE ROWNUM <= {sample_size}")
        new_rows = []
        for row in rows:
            # print(row)
            row_convert = []
            for value in list(row):
                if isinstance(value, datetime):
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(value, date):
                    value = value.strftime('%Y-%m-%d')
                elif isinstance(value, bytes):
                    value = value.decode('utf-8', errors='ignore')
                elif isinstance(value, oracledb.LOB):
                    value = value.read()
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='ignore')
                row_convert.append(value)
            new_rows.append(row_convert)
        return new_rows

    def close(self):
        self.oracle_cursor.close()
        self.oracle_conn.close()

class PostgreSQLConnector:

    db_type = 'PostgreSQL'

    def __init__(self, dbconfig):
        self.pg_conn = psycopg2.connect(**dbconfig)
        self.pg_cursor = self.pg_conn.cursor()

    def change_database(self, dbconfig):
        self.pg_conn = psycopg2.connect(**dbconfig)
        self.pg_cursor = self.pg_conn.cursor()

    def execute_dml(self, sql):
        self.pg_cursor.execute(sql)
        self.pg_conn.commit()
        print(f"成功执行 {sql} 语句")

    def query_data(self, sql):
        self.pg_cursor.execute("SAVEPOINT try_query")
        try:
            self.pg_cursor.execute(sql)
            rows = self.pg_cursor.fetchall()
            columns = [col[0] for col in self.pg_cursor.description]  # 获取列名
            self.pg_cursor.execute("RELEASE SAVEPOINT try_query")
            return rows, columns
        except psycopg2.errors.InsufficientPrivilege as e:
            # 权限错误：回滚到保存点，继续下一个
            self.pg_cursor.execute("ROLLBACK TO SAVEPOINT try_query")
            # print(f"{sql}查询 无权限")
            return [], []

    def close(self):
        self.pg_cursor.close()
        self.pg_conn.close()

class ClickHouseConnector:

    db_type = 'ClickHouse'

    def __init__(self, dbconfig):
        self.client = Client(**dbconfig)

    def query_data(self, sql, settings={'max_block_size': 10000}):
        rows, column_types = self.client.execute(sql, with_column_types=True)
        columns = [col[0] for col in column_types]
        return rows, columns

    def close(self):
        self.client.disconnect()

class DaMengConnector:
    db_type = 'DaMeng'

    def __init__(self, dbconfig):
        self.conn = dmPython.connect(**dbconfig)
        self.cursor = self.conn.cursor()

    def query_data(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return rows, columns

    def close(self):
        # 关闭连接
        self.cursor.close()
        self.conn.close()

class SQLiteConnector:
    db_type = 'SQLite'

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        self.cursor = self.conn.cursor()

    def query_data(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return rows, columns

    def execute_ddl(self, sql):
        self.cursor.execute(sql)

    def insert_data(self, insert_query, data):
        self.cursor.executemany(insert_query, data)
        self.conn.commit()
        
    def close(self):
        # 关闭连接
        self.cursor.close()
        self.conn.close()

class DuckDBConnector:

    db_type = 'DuckDB'

    def __init__(self, db_file=''):
        # 连接到内存数据库
        self.conn = duckdb.connect(db_file)

    def query_data(self, sql):
        cursor = self.conn.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return rows, columns

    def execute_ddl(self, sql):
        self.conn.execute(sql)

    def insert_data(self, insert_query, data):
        self.conn.executemany(insert_query, data)

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

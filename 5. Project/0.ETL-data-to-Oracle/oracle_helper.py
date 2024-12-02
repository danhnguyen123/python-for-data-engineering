import cx_Oracle
import numpy, datetime, logging, os
from sqlalchemy import types, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import VARCHAR, DATE, DATETIME, FLOAT, INT, NVARCHAR, TIMESTAMP
from math import isnan
import pandas as pd
from tqdm import tqdm

class OracleHelper:

    def __init__(self, config, max_connection_pool = 10):

        self.config = config

        try:
            # self.connection = cx_Oracle.connect(self.config['user'], self.config['pwd'], self.config['host'])
            cx_Oracle.init_oracle_client(lib_dir=os.path.join(os.getenv('ORACLE_CLIENT_LIB')))

            self.pool = cx_Oracle.SessionPool(self.config['user'], self.config['pwd'], self.config['host'],
                             min = 1, max = max_connection_pool, increment = 1, threaded = True,
                             getmode = cx_Oracle.SPOOL_ATTRVAL_WAIT,
                             sessionCallback = self.initSession)

            # self.cursor = self.connection.cursor()

            connect_string = 'oracle+cx_oracle://{}:{}@{}:{}/?service_name={}'.format(
                self.config['user'], self.config['pwd'], self.config['ip'],
                self.config['port'], self.config['service_name']
                )
            
            self.connection = create_engine(connect_string
                , max_identifier_length=128
                , pool_size=20, max_overflow=0)
            # logging.info('DB connected')

        except Exception as e:
            print(e, self.config['host'])
            raise(e)

    # Set the NLS_DATE_FORMAT for a session
    def initSession(self,connection, requestedTag):
        cursor = connection.cursor()
        # query = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS', NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI' ")
        connection.commit()

    def InConverter(self, value):
        return int(value)  # or whatever is needed to convert from numpy.int64 to an integer

    def InputTypeHandler(self, cursor, value, num_elements):
        if isinstance(value, numpy.int64):
            return cursor.var(int, arraysize=num_elements, inconverter=self.InConverter)

    def check_table(self, table_name):
        try:
            connection = self.pool.acquire()
            cursor = connection.cursor()
            cursor.execute(f"select * from {table_name} FETCH FIRST 5 ROWS ONLY")
            return True
        except:
            return False

    def write_df_to_db(self, df: pd.DataFrame, table_name):
        con = self.pool.acquire()
        df.to_sql(table_name, con, if_exists='append')

    def execute_query(self, query, values = []):
        # print(query, values)
        connection = self.pool.acquire()
        cursor = connection.cursor()
        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
        connection.commit()

        try:
            print(query)
            cursor.inputtypehandler = self.InputTypeHandler
            result = cursor.execute(query, values)
            connection.commit()
            print("excecute done")
            # Release the connection to the pool
            self.pool.release(connection)
            return True, None
        except cx_Oracle.DatabaseError as e:
            error_obj, = e.args
            print("Error Code:", error_obj.code)
            print("Error Message:", error_obj.message)
            # print(e, query, values)
            return False, repr(error_obj.message)

        # return result, query
        
    def execute_to_df(self, query):
        connection = self.pool.acquire()
        try:
            df = pd.read_sql(query, connection)
            return df
        except cx_Oracle.DatabaseError as e:
            error_obj, = e.args
            print("Error Code:", error_obj.code)
            print("Error Message:", error_obj.message)

    def mapping_type(self, dtype):
        if dtype.lower() == 'int64':
            return ' number(30)'
        elif dtype.lower() == 'float64':
            return ' float(30)'
        elif dtype.lower() == 'object':
            return ' varchar2(2000)'
        elif dtype.lower() == 'datetime64[ns]':
            return ' timestamp'
        else:
            return ' varchar2(2000)'

    def insert(self, df, table_name, unique_col=[], date_col=[], date_format=[]):
        append_rec = []
        cols = df.columns
        types_statement = ''
        value_statement = ''
        connection = self.pool.acquire()
        cursor = connection.cursor()
        for i, each_col in enumerate(cols):
            # print(each_col)
            types_statement += str(each_col).lower() + (" timestamp" if str(each_col) in date_col else self.mapping_type(str(df[each_col].dtype))) + ','

            value_statement += f":{i+1},"

        if self.check_table(table_name) == False:
            unique_statement = ''
            if len(unique_col) != 0:
                if '.' in table_name:
                    unique_statement += f",CONSTRAINT {table_name.split('.')[1].lower()}_unique UNIQUE ({','.join(unique_col)})"
                else:
                    unique_statement += f",CONSTRAINT {table_name.lower()}_unique UNIQUE ({','.join(unique_col)})"
            print(f"""CREATE TABLE {table_name.lower()} ({types_statement[:-1]} {unique_statement})""")
            cursor.execute(f"""CREATE TABLE {table_name.lower()} ({types_statement[:-1]} {unique_statement})""")
            connection.commit()

        for _, row in tqdm(df.iterrows(), total=len(df)):
            each_rec = []
            for i, each_col in enumerate(cols):
                if row[each_col] is None or str(row[each_col]) == 'nan':
                    each_rec.append(None)
                else:
                    if df[each_col].dtype in ('int64', 'float64'):
                        each_rec.append(float(row[each_col]))
                    else:
                        if each_col in date_col:
                            # if each_col == 'sap_date':
                            position = date_col.index(each_col)
                            format_ = date_format[position]
                            if str(row[each_col]).lower() != 'nat' and row[each_col] is not None:
                                try:
                                    converted_date = datetime.datetime.strptime(str(row[each_col]), format_ )
                                except:
                                    converted_date = datetime.datetime.strptime(str(row[each_col]).split('.')[0], format_ )
                            else:
                                converted_date = None
                            each_rec.append(converted_date)
                        else:
                            each_rec.append(str(row[each_col]))

            append_rec.extend([tuple(each_rec)])

        insert_statement = f"insert into {table_name} values({value_statement[:-1]})"
        # print(insert_statement)

        cursor.executemany(insert_statement, append_rec)
        connection.commit()

    def drop_table(self,table_name):
        query = """BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE {}';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;""".format(table_name)
        return self.execute_query(query)

    def close(self):
        self.pool.close()
    
    def truncate_table(self, table_name):
        connection = self.pool.acquire()
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        print(f"truncate table {table_name}")

        
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, staging_events_table_drop, staging_songs_table_drop, quality_check, total_rows


def load_staging_tables(cur, conn):
    '''
    Copy data from source to target tables
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Staging tables are loaded!')

def insert_tables(cur, conn):
    '''
    Bulk insert data from staging tables to target tables
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Data is imported!')

def drop_staging_tables(cur, conn):
    '''
    Drop staging tables after ETL process.
    '''
    for query in [staging_events_table_drop, staging_songs_table_drop]:
        cur.execute(query)
        conn.commit()
    
def data_quality_check(cur):
    '''
    Check the data quality after ETL.
    - Duplicate data for PARIMARY KEYS.
    '''
    lst_error = []
    for query in quality_check:
        cur.execute(query)
        rows = cur.fetchall()
        if rows[0][1] > 0:
            lst_error.append(rows)
    return lst_error

def row_count(cur):
    '''
    Execute queies to count number of rows for each table after ETL process.
    '''
    cur.execute(total_rows)
    rows = cur.fetchall()
    print('table_name', 'total_rows')
    for row in rows:
        print(row[0], row[1])
    
    
def main():
    '''
    The main function of the ETL pipline. It contains steps below.
    - Load staging tables.
    - Insert data from staging tables into the data warehouse.
    - Drop staging tables.
    - Conduct data quality checkup.
    - Print number of rows imported for each table in the data warehouse.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print('====================')
    insert_tables(cur, conn)
    drop_staging_tables(cur, conn)
    
    # Check the data quality after ETL.
    lst_error = data_quality_check(cur)
    
    # Totol rows imported.
    print('====================')
    row_count(cur)
    print('====================')
    conn.close()
    
    if lst_error != []:
        print('There are some duplicates!')
        for e in lst_error:
            print(e)


if __name__ == "__main__":
    main()
    print('====================')
    print('The ETL process is completed!')
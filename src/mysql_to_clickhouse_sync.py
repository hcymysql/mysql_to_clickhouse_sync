#!/usr/bin/env python3
# MySQL全量数据导入到ClickHouse里，默认并行10张表同时导出数据，每次轮询取1000条数据。
# 使用条件：表必须有自增主键，测试环境MySQL 8.0
# python3 mysql_to_clickhouse_sync.py --mysql_host 192.168.198.239 --mysql_port 3336 --mysql_user admin --mysql_password hechunyang --mysql_db hcy --clickhouse_host 192.168.176.204 --clickhouse_port 9000 --clickhouse_user hechunyang --clickhouse_password 123456 --clickhouse_database hcy --batch_size 1000 --max_workers 10

import argparse
import pymysql.cursors
from clickhouse_driver import Client
from concurrent.futures import ThreadPoolExecutor
import datetime
import logging
import sys

# 创建日志记录器，将日志写入文件和控制台
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('sync.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def read_from_mysql(table_name, start_id, end_id, mysql_config):
    mysql_connection = pymysql.connect(**mysql_config, cursorclass=pymysql.cursors.DictCursor)
    try:
        with mysql_connection.cursor() as cursor:
            query = "SELECT * FROM {} WHERE _rowid >= {} AND _rowid < {}".format(table_name, start_id, end_id)
            cursor.execute(query)
            results = cursor.fetchall()
            return results
    finally:
        mysql_connection.close()

def insert_into_clickhouse(table_name, records, clickhouse_config):
    clickhouse_client = Client(**clickhouse_config)
    try:
        column_names = list(records[0].keys())
        values_list = []
        for record in records:
            values = []
            for column_name in column_names:
                value = record[column_name]
                if isinstance(value, str):
                    value = value.replace("'", "''")
                    values.append(f"'{value}'")
                elif isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                    values.append(f"'{value}'")
                elif value is None:
                    values.append("NULL")
                elif isinstance(value, (int, float)):
                    values.append(str(value))
                else:
                    values.append(f"'{str(value)}'")
            values_list.append(f"({','.join(values)})")
        query = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES {','.join(values_list)}"
        clickhouse_client.execute(query)
        ###调试使用
        # logger.info(f"执行的SQL是：{query}")
    except Exception as e:
        logger.error('Error inserting records into ClickHouse:', e)
    finally:
        clickhouse_client.disconnect()

def worker(table_name, table_bounds, mysql_config, clickhouse_config, batch_size, max_workers):
    min_id, max_id = table_bounds[table_name]
    if min_id == max_id:  # 如果表只有一条记录，则直接处理
        records = read_from_mysql(table_name, min_id, max_id + 1, mysql_config)
        print(f"Retrieved {len(records)} record from MySQL table {table_name} with ID {min_id}")
        if len(records) > 0:
            insert_into_clickhouse(table_name, records, clickhouse_config)
        return

    row_count = max_id - min_id + 1
    if row_count <= 1000:  # 如果行数小于等于 1000，则将批处理大小设置为行数
        batch_size = row_count
    else:
        batch_size = batch_size

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for start_id in range(min_id, max_id, batch_size):
            end_id = start_id + batch_size
            if end_id > max_id:
                end_id = max_id + 1
            records = read_from_mysql(table_name, start_id, end_id, mysql_config)
            print(f"Retrieved {len(records)} records from MySQL table {table_name} between ID {start_id} and {end_id}")
            if len(records) > 0:
                executor.submit(insert_into_clickhouse, table_name, records, clickhouse_config)

def main(args):
    mysql_config = {
        'host': args.mysql_host,
        'port': args.mysql_port,
        'user': args.mysql_user,
        'password': args.mysql_password,
        'db': args.mysql_db,
        'charset': 'utf8mb4'
    }

    clickhouse_config = {
        'host': args.clickhouse_host,
        'port': args.clickhouse_port,
        'user': args.clickhouse_user,
        'password': args.clickhouse_password,
        'database': args.clickhouse_database
    }

    mysql_connection = pymysql.connect(**mysql_config, autocommit=False, cursorclass=pymysql.cursors.DictCursor)
    mysql_connection.begin()
    try:
        with mysql_connection.cursor() as cursor:
            cursor.execute("SET transaction_isolation = 'REPEATABLE-READ'")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")  # 设置一致性快照
            cursor.execute("SHOW TABLES")
            result = cursor.fetchall()
            tables = [val for d in result for val in d.values()]
            table_bounds = {}
            for table_name in tables:
                cursor.execute("SELECT MIN(_rowid) AS `MIN(id)`, MAX(_rowid) AS `MAX(id)` FROM `{}`".format(table_name))
                row = cursor.fetchone()
                min_id, max_id = row['MIN(id)'], row['MAX(id)']
                table_bounds[table_name] = (min_id, max_id)

            cursor.execute("SHOW MASTER STATUS")  # 获取当前的binlog文件名和位置点信息
            binlog_row = cursor.fetchone()
            binlog_file, binlog_position, gtid = binlog_row['File'], binlog_row['Position'], binlog_row['Executed_Gtid_Set']

            # 将binlog文件名、位置点和GTID信息保存到metadata.txt文件中
            with open('metadata.txt', 'w') as f:
                f.write('{}\n{}\n{}'.format(binlog_file, binlog_position, gtid))
    except Exception as e:
        logger.error(e)

    tables = table_bounds.keys()

    # 并发十张表同时导入数据
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        for table_name in tables:
            executor.submit(worker, table_name, table_bounds, mysql_config, clickhouse_config, args.batch_size, args.max_workers)

def parse_args():
    parser = argparse.ArgumentParser(description='MySQL to ClickHouse data synchronization')
    parser.add_argument('--mysql_host', type=str, required=True, help='MySQL host')
    parser.add_argument('--mysql_port', type=int, required=True, help='MySQL port')
    parser.add_argument('--mysql_user', type=str, required=True, help='MySQL username')
    parser.add_argument('--mysql_password', type=str, required=True, help='MySQL password')
    parser.add_argument('--mysql_db', type=str, required=True, help='MySQL database')
    parser.add_argument('--clickhouse_host', type=str, required=True, help='ClickHouse host')
    parser.add_argument('--clickhouse_port', type=int, required=True, help='ClickHouse port')
    parser.add_argument('--clickhouse_user', type=str, required=True, help='ClickHouse username')
    parser.add_argument('--clickhouse_password', type=str, required=True, help='ClickHouse password')
    parser.add_argument('--clickhouse_database', type=str, required=True, help='ClickHouse database')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for data import (default: 1000)')
    parser.add_argument('--max_workers', type=int, default=10, help='Maximum number of worker threads (default: 10)')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(args)

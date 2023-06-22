python3 mysql_to_clickhouse_sync.py --help

usage: mysql_to_clickhouse_sync.py [-h] --mysql_host MYSQL_HOST --mysql_port MYSQL_PORT --mysql_user MYSQL_USER --mysql_password MYSQL_PASSWORD
                                   --mysql_db MYSQL_DB --clickhouse_host CLICKHOUSE_HOST --clickhouse_port CLICKHOUSE_PORT --clickhouse_user
                                   CLICKHOUSE_USER --clickhouse_password CLICKHOUSE_PASSWORD --clickhouse_database CLICKHOUSE_DATABASE
                                   [--batch_size BATCH_SIZE] [--max_workers MAX_WORKERS]

### MySQL to ClickHouse data synchronization

options:

  -h, --help            show this help message and exit
  
  --mysql_host MYSQL_HOST
                        MySQL host
                        
  --mysql_port MYSQL_PORT
                        MySQL port
                        
  --mysql_user MYSQL_USER
                        MySQL username
                        
  --mysql_password MYSQL_PASSWORD
                        MySQL password
                        
  --mysql_db MYSQL_DB   MySQL database
  
  --clickhouse_host CLICKHOUSE_HOST
                        ClickHouse host
                        
  --clickhouse_port CLICKHOUSE_PORT
                        ClickHouse port
                        
  --clickhouse_user CLICKHOUSE_USER
                        ClickHouse username
                        
  --clickhouse_password CLICKHOUSE_PASSWORD
                        ClickHouse password
                        
  --clickhouse_database CLICKHOUSE_DATABASE
                        ClickHouse database
                        
  --batch_size BATCH_SIZE
                        Batch size for data import (default: 1000)
                        
  --max_workers MAX_WORKERS
                        Maximum number of worker threads (default: 10)
                        

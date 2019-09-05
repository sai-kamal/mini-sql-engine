import sys
from db import DB
from query import Query

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('syntax: python3 20161133.py `query`')
    
    # create database
    db = DB()
    # query
    q = Query(sys.argv[1])
    q.process_query()
    print(q.tables)
    print(q.columns)
    print(q.conds)
    
    # post process using DB and query
    db.post_check(q)
    table = db.run_query(q)
    table = db.process_agg(table, q)
    db.print_table(table, q)

import sys
from db import DB


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('syntax: python3 20161133.py `query`')
    
    # create database
    db = DB()
    
    # query
    result = db.process_queries(sys.argv[1])

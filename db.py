import sqlparse as sp
from sqlparse.tokens import *
from sqlparse.sql import *

from query import Query

class DB:
    '''creates and stores the DB from the provided files'''
    def __init__(self):
        '''inits all variables for the DB'''
        self.tables = dict()
        self.make_tables()
        self.update_tables()
        # print(self.tables)

    def make_tables(self):
        '''creates empty tables from metadata.txt'''
        try:
            with open('metadata.txt') as fp:
                meta = fp.read().strip().split('\n')
        except IOError:
            print("metadata.txt doesn't exist")
            exit(0)
        i = 0
        while i < len(meta):
            line = meta[i].strip()
            if line == '<begin_table>':
                table = dict()
                i += 1
                table_name = meta[i].strip()
                i += 1
                while meta[i] != '<end_table>':
                    col = meta[i].strip()
                    table[col] = list()
                    i += 1
                self.tables[table_name] = table
                i += 1
            else:
                i += 1

    def update_tables(self):
        '''update the empty tables using their corresponding files'''
        for k, v in self.tables.items():
            self.update_table(k, v)

    def update_table(self, table_name, table_dict):
        '''update one empty table using its file'''
        cols = list(table_dict.keys())
        try:
            with open(table_name + '.csv') as fp:
                lines = fp.readlines()
        except IOError:
            print(table_name + " file doesn't exist")
            exit(0)
        for line in lines:
            line = line.strip().split(',')
            for i, val in enumerate(line):
                table_dict[cols[i]].append(int(val))
    
    def post_check(self):
        '''after processing check if anything else is missing'''
        # check for select, *, columns, from, table and if their validity
        pass

    def print_table(self, table_name, table):
        #TODO
        '''print the resultant table in the required format'''
        pass

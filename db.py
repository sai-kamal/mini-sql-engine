import re
import sqlparse as sp
from sqlparse.tokens import *
from sqlparse.sql import *


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
    
    # def print_table(self, table_name, table):
    #     #TODO
    #     '''print the resultant table in the required format'''
    #     pass

    def process_queries(self, queries):
        '''process the queries'''
        parsed = sp.parse(queries)
        for stmt in parsed:
            # get a return val and send that for post processsing
            self.process_stmt(stmt)
            self.post_process()
            # perform the query
            # print the result

    def process_stmt(self, stmt):
        '''process a single stmt'''
        print(stmt.tokens)
        seen_select = 0
        seen_from = 0
        seen_where = 0
        seen_star = 0
        seen_distinct = 0
        columns = []
        tables = []
        for token in stmt.tokens:
            if token.ttype is DML:
                if token.match(token.ttype, r'[sS][eE][lL][eE][cC][tT]', True):
                    seen_select = 1
                    print("select is seen")
                else:
                    print('unknown DML seen!!!', token)
                    exit(0)
            
            elif token.ttype is Whitespace or token.ttype is Punctuation:
                continue
            
            elif token.ttype is Wildcard:
                if token.match(token.ttype, ['*']):
                    seen_star = 1
                    print("star is seen")
                else:
                    print('unknown wildcard seen!!!', token)
                    exit(0)

            elif token.ttype is Keyword:
                if token.match(token.ttype, r'[dD][iI][sS][tT][iI][nN][cC][tT]', True):  # distinct - no brackets
                    if seen_distinct or columns or seen_star:
                        print('before this */distinct/columns are already present, please check the query')
                        exit(0)
                    seen_distinct = 1
                    print("distinct is seen")
                elif token.match(token.ttype, r'[fF][rR][oO][mM]', True):  # from
                    seen_from = 1
                    print("from is seen")
                else:
                    print('unknown keyword seen!!!', token)
                    exit(0)

            elif isinstance(token, IdentifierList):
                if seen_select and not seen_from:
                    columns = self.process_id_list(token, 'columns')
                elif seen_from and not seen_where:
                    tables = self.process_id_list(token, 'tables')
                else:
                    print("unknown identifier list seen in query", token)
            
            elif isinstance(token, Identifier):
                if seen_select and not seen_from:
                    columns = [self.process_id(token, 'columns')]
                elif seen_from and not seen_where:
                    tables = [self.process_id(token, 'tables')]
                else:
                    print('unknown identifier list seen!!!', token)

            elif isinstance(token, Function):
                print('function is seen')
                if seen_select and not seen_from and len(columns) == 0:
                    columns = [self.process_id(token, 'columns')]
                else:
                    print('function seen - incorrect query!!!', token)

            elif isinstance(token, Where):
                seen_where = 1
                print('where is seen')
                where = self.process_where(token)
            
            else:
                print('unknown token seen in the whole query!!!', token)

            print(tables)
            print(columns)
            # return the flags, tables, columns, where

    def process_id_list(self, token, typ):
        '''process IdentifierList in query'''
        print('id list ', token.tokens)
        arr = []
        for identifier in token.tokens:
            if identifier.ttype is Whitespace or identifier.ttype is Punctuation:
                continue
            # elif identifier.ttype is Function or identifier.match(identifier.ttype, ['*']):
            #     # function and * in identifiers list case is ignored and error is shown
            #     print("aggregate function/* case in identifiers list is ignored!!!")
            #     exit(0)
            elif isinstance(identifier, Identifier):  # only identifiers are present in identifiers list and should be explored
                ret = self.process_id(identifier, typ)
                ret['type'] = typ
                arr.append(ret)
            else:
                print("unknown token in identifiers list is ignored!!!", identifier)
                exit(0)
        return arr

    def process_id(self, token, typ):
        '''process IdentifierList in query, every token/id will be a dict denoting the value and 
        any aggregate function on it and the table to be used
        A dict is returned'''
        tkn_info = dict()
        if typ == 'columns':
            # if isinstance(token, Identifier) or isinstance(token, Function): # for single function call directly from process_stmt
            tkn_info = self.split_col_token(token.value)
            # else:
            #     print('unknown token as column identifier!!!', token, token.ttype)
            #     exit(0)
        elif typ == 'tables':
            # if isinstance(token, Identifier):
            tkn_info['table'] = token.value
            # else:
            #     print('unknown token as table identifier!!!', token, token.ttype)
            #     exit(0)
        else:
            print('unknow type in process_id', typ)
        return tkn_info
    
    def split_col_token(self, token):
        '''if table name is present in token it is split along with the function'''
        tkn_info = dict()
        token = token.split('(')
        # if function exists and assuming only a single function
        if len(token) > 1:
            tkn_info['function'] = token[0]
            token = token[1][:-1]
        else:
            token = token[0]
        # if table name exists as table1.A
        token = token.split('.')
        if len(token) > 1:
            tkn_info['table'] = token[0]
            tkn_info['col'] = token[1]
        else:
            tkn_info['col'] = token[0]
        return tkn_info
    
    def process_where(self, token):
        '''process where part of the query/stmt'''
        pass

    def post_process(self):
        '''after processing check if anything else is missing'''
        # check for select, *, columns, from, table and if their validity
        pass

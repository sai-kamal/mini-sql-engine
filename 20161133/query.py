import sqlparse as sp
from sqlparse.tokens import *
from sqlparse.sql import *

class Query:
    def __init__(self, query):
        ''' init method'''
        self.query = query
        self.seen_select = 0
        self.seen_from = 0
        self.seen_where = 0
        self.seen_star = 0
        self.seen_distinct = 0
        self.columns = []
        self.tables = []
        self.conds = []
        self.rel_2_conds = 0  # 0 for no relation between 2 conds, 1 for OR, 2 for AND

    def process_query(self):
        '''process the queries'''
        parsed = sp.parse(self.query)
        for stmt in parsed:
            # get a return val and send that for post processsing
            self.process_stmt(stmt)

    def process_stmt(self, stmt):
        '''process a single stmt'''
        # print(stmt.tokens)
        for token in stmt.tokens:
            if token.ttype is DML:
                if token.match(token.ttype, r'[sS][eE][lL][eE][cC][tT]', True):
                    self.seen_select = 1
                    # print("select is seen")
                else:
                    print('unknown DML seen!!!', token)
                    exit(0)

            elif token.ttype is Whitespace or token.ttype is Punctuation:
                continue

            elif token.ttype is Wildcard:
                if token.match(token.ttype, ['*']):
                    self.seen_star = 1
                    # print("star is seen")
                else:
                    print('unknown wildcard seen!!!', token)
                    exit(0)

            elif token.ttype is Keyword:
                # distinct - no brackets
                if token.match(token.ttype, r'[dD][iI][sS][tT][iI][nN][cC][tT]', True):
                    if self.seen_distinct or len(self.columns) or self.seen_star:
                        print(
                            'before this */distinct/columns are already present, please check the query')
                        exit(0)
                    self.seen_distinct = 1
                    # print("distinct is seen")
                elif token.match(token.ttype, r'[fF][rR][oO][mM]', True):  # from
                    self.seen_from = 1
                    # print("from is seen")
                else:
                    print('unknown keyword seen!!!', token)
                    exit(0)

            elif isinstance(token, IdentifierList):
                if self.seen_select and not self.seen_from:
                    self.columns = self.process_id_list(token, 'columns')
                elif self.seen_from and not self.seen_where:
                    self.tables = self.process_id_list(token, 'tables')
                else:
                    print("unknown identifier list seen in query", token)

            elif isinstance(token, Identifier):
                if self.seen_select and not self.seen_from:
                    self.columns = [self.process_id(token, 'columns')]
                elif self.seen_from and not self.seen_where:
                    self.tables = [self.process_id(token, 'tables')]
                else:
                    print('unknown identifier list seen!!!', token)

            elif isinstance(token, Function):
                # print('function is seen', token.value)
                if self.seen_select and not self.seen_from and len(self.columns) == 0:
                    self.columns = [self.process_id(token, 'columns')]
                else:
                    print('function seen - incorrect query!!!', token)

            elif isinstance(token, Where):
                self.seen_where = 1
                where = self.process_where(token)

            else:
                print('unknown token seen in the whole query!!!', token)
        # return the flags, tables, columns, where

    def process_id_list(self, token, typ):
        '''process IdentifierList in query'''
        # print('id list ', token.tokens)
        arr = []
        for identifier in token.tokens:
            if identifier.ttype is Whitespace or identifier.ttype is Punctuation:
                continue
            # only identifiers are present in identifiers list and should be explored
            elif isinstance(identifier, Identifier):
                ret = self.process_id(identifier, typ)
                # ret['type'] = typ
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
            tkn_info = self.split_col_token(token.value)
        elif typ == 'tables':
            tkn_info['table'] = token.value
        else:
            print('unknow type in process_id', typ)
        return tkn_info

    def split_col_token(self, token):
        '''if table name is present in token it is split along with the function'''
        tkn_info = dict()
        token = token.split('(')
        # if function exists and assuming only a single function
        if len(token) > 1:
            # if token[0] == 'distinct':
            #     print('distinct can\'t be used as a function!!!')
            #     exit(0)
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
        # if tkn_info['col'] == '*':
        #     self.seen_star = 1
            # print('wrong usage of *!!!')
            # exit(0)
        return tkn_info

    def process_where(self, token):
        '''process where part of the query/stmt'''
        # print(token.tokens)
        for tkn in token.tokens:
            if tkn.ttype is Whitespace or tkn.ttype is Punctuation or tkn.match(Keyword, r'[wW][hH][eE][rR][eE]', True):
                continue
            elif tkn.ttype is Keyword:
                if tkn.match(tkn.ttype, r'[oO][rR]', True):
                    self.rel_2_conds = 1  # OR
                elif tkn.match(tkn.ttype, r'[aA][nN][dD]', True):
                    self.rel_2_conds = 2  # AND
                else:
                    print('unknown keyword seen in WHERE!!!', tkn)
                    exit(0)
            elif isinstance(tkn, Comparison):
                self.conds.append(self.process_cond(tkn))
            else:
                print('unknown token seen in the where part of query!!!', tkn)
                exit(0)

    def process_cond(self, cond):
        '''process a Comparison class'''
        cond_dict = dict()
        id_cnt = 0
        # print(cond.tokens)
        for token in cond.tokens:
            if token.ttype is Whitespace :
                continue
            elif isinstance(token, Identifier):
                cond_dict['id' + str(id_cnt)] = self.process_id(token, 'columns')
                id_cnt += 1
            elif token.match(token.ttype, r'<|>|<=|>=|=', True):
                cond_dict['op'] = token.value
            elif token.match(token.ttype, r'[0-9]+', True):
                cond_dict['val'] = token.value
            else:
                print('unknown token seen in the condition!!!', token.ttype, type(token))
                exit(0)
        cond_dict['id_cnt'] = id_cnt
        return cond_dict

    def join_check(self):
        ''' check if any cond is based on inner join'''
        for cond_dict in self.conds:
            if 'id1' in cond_dict and cond_dict['op'] == '=' and self.seen_star:
                col = cond_dict['id1']
                col_name = col['table'] + '.' + col['col']
                # remove the extra column
                self.columns.remove(col)

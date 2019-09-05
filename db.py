import re
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
        if not len(lines):
            print('no data in the file ', table_name)
            exit(0)
        for line in lines:
            line = line.strip().split(',')
            for i, val in enumerate(line):
                table_dict[cols[i]].append(int(val))
    
    def post_check(self, q):
        '''after processing check if anything else is missing'''
        # check if select and from are present
        if not q.seen_select or not q.seen_from or not len(q.tables):
            print('query is not complete!!!')
            exit(0)
        # check if any column or * is present
        if not q.seen_star and not len(q.columns):
            print('no columns in query!!!')
            exit(0)
        # check if query tables are in db
        self.tables_in_query = []  # table_dicts for tables present in query
        for table_dict in q.tables:
            if table_dict['table'] not in self.tables.keys():
                print(table_dict['table'] + ' not in given DB')
                exit(0)
            else:
                self.tables_in_query.append(self.tables[table_dict['table']])
        # check if query columns in select part of the query are in DB and 
        for col_dict in q.columns:
            self.check_col_validity(q, col_dict)
        # 'where' is present but conds are not
        if q.seen_where and not len(q.conds):
            print('"where" is present,but no conds, query incomplete!!!')
            exit(0)
        elif q.seen_where and len(q.conds):
            # check if query columns in where part of the query are in DB
            if len(q.conds) != 1 and len(q.conds) != 2:
                print('number of conditions is greater than 2, invalid!!!')
                exit(0)
            for cond_dict in q.conds:
                # if there is no column in the condition
                if not cond_dict['id_cnt']:
                    print('no column in condition!!!')
                    exit(0)
                for i in range(cond_dict['id_cnt']):
                    col_dict = cond_dict['id' + str(i)]
                    self.check_col_validity(q, col_dict)
        # if * is seen, change q.columns to have all columns
        if q.seen_star:
            q.columns = []
            for i, table_dict in enumerate(self.tables_in_query):
                table_name = q.tables[i]['table']
                for col_name in table_dict.keys():
                    q.columns.append({'table': table_name, 'col': col_name})

    def check_col_validity(self, q, col_dict):
        '''check the validity of a column in query and DB'''
        if 'table' in col_dict:
            tbl = col_dict['table']
            if tbl not in self.tables.keys() or col_dict['col'] not in self.tables[tbl].keys() or self.tables[tbl] not in self.tables_in_query:
                print(tbl + ' or col ' +
                    col_dict['col'] + ' not in given DB')
                exit(0)
        else:
            col_present = 0
            table_name = ''
            for i, table_dict in enumerate(self.tables_in_query):
                if col_dict['col'] in table_dict.keys():
                    col_present += 1
                    table_name = q.tables[i]['table']
            if col_present > 1:
                print(
                    col_dict['col'] + ' present in more than one table; query is ambiguous')
                exit(0)
            else: # add the column name to the col_dict as it is not present
                col_dict['table'] = table_name

    def run_query(self, q):
        '''run the query on the DB'''
        # first combine all the tables present in the query to form a new dict
        final_table = self.cross_product_tables(q)
        # if 'where' is present in query, execute those conds
        if q.seen_where:
            # add extra flag column
            length = len(list(final_table.values())[0])
            final_table['valid'] = [0]*length # all are not valid initial
            # process first condition
            self.process_cond(final_table, q.conds[0])
            if len(q.conds) == 2:
                self.process_cond(final_table, q.conds[1], q.rel_2_conds)

        # make output table only have valid entries
        output_table = dict()
        if 'valid' in final_table:
            for i, val in enumerate(final_table['valid']):
                if val:
                    for col_dict in q.columns:
                        col = col_dict['table'] + '.' + col_dict['col']
                        if col not in output_table.keys():
                            output_table[col] = []
                        output_table[col].append(final_table[col][i])
        else:
            for i in range(len(list(final_table.values())[0])):
                for col_dict in q.columns:
                    col = col_dict['table'] + '.' + col_dict['col']
                    if col not in output_table.keys():
                        output_table[col] = []
                    output_table[col].append(final_table[col][i])
        return output_table

    def cross_product_tables(self, q):
        '''join all the columns of tables present in the query'''
        final_table = dict()
        if len(q.tables) == 1:
            table_dict = self.tables_in_query[0]
            table_name = q.tables[0]['table']
            for col, col_arr in table_dict.items():
                final_table[table_name + '.' + col] = col_arr
        elif len(q.tables) == 2:
            table1_name = q.tables[0]['table']
            table1 = self.tables_in_query[0]
            table1_len = len(list(table1.values())[0])
            
            table2_name = q.tables[1]['table']
            table2 = self.tables_in_query[1]
            table2_len = len(list(table2.values())[0])
            
            for col, col_arr in table1.items():
                final_table[table1_name + '.' + col] = []
                for val in col_arr:
                    final_table[table1_name + '.' + col] += [val]*table2_len
            
            for col, col_arr in table2.items():
                final_table[table2_name + '.' + col] = col_arr*table1_len
        else:
            print('number of tables in query is not 1 or 2!!!')
            exit(0)
        return final_table

    def process_cond(self, table, cond_dict, rel=None):
        '''process given cond on table'''
        if 'op' not in cond_dict:
            print('operator not present')
            exit(0)
        op = cond_dict['op']
        if cond_dict['op'] == '=':
            op = '=='

        if cond_dict['id_cnt'] == 1:
            if 'val' not in cond_dict:
                print('value to be compared with not present')
                exit(0)
            val = cond_dict['val']
            col_name = cond_dict['id0']['table'] + '.' + cond_dict['id0']['col']
            for i, col_val in enumerate(table[col_name]):
                eval_str = str(col_val) + op + val
                if rel is None and eval(eval_str):
                        table['valid'][i] = 1
                elif rel == 1:
                    if table['valid'][i] or eval(eval_str):  # or
                        table['valid'][i] = 1
                else:
                    if table['valid'][i] and eval(eval_str):  # and
                        table['valid'][i] = 1
        
        elif cond_dict['id_cnt'] == 2:
            col1_name = cond_dict['id0']['table'] + '.' + cond_dict['id0']
            ['col']
            col2_name = cond_dict['id1']['table'] + '.' + cond_dict['id1']
            ['col']
            length = len(table[col1_name]) * len(table[col2_name])
            for i in range(length):
                eval_str = str(table[col1_name][i]) + \
                    op + str(table[col2_name][i])
                if rel is None and eval(eval_str):
                    table['valid'][i] = 1
                elif rel == 1:
                    if table['valid'][i] or eval(eval_str):  # or
                        table['valid'][i] = 1
                else:
                    if table['valid'][i] and eval(eval_str):  # and
                        table['valid'][i] = 1

    def process_agg(self, table, q):
        ''' processes aggregations if any on the columns'''
        for col_dict in q.columns:
            if 'function' in col_dict:
                func = col_dict['function']
                col = col_dict['table'] + '.' + col_dict['col']
                if re.match(r'[mM][aA][xX]', func):
                    table[col] = [max(table[col])]
                elif re.match(r'[mM][iI][nN]', func):
                    table[col] = [min(table[col])]
                elif re.match(r'[sS][uU][mM]', func):
                    table[col] = [sum(table[col])]
                elif re.match(r'[aA][vV][gG]', func):
                    table[col] = [sum(table[col])/len(table[col])]
                else:
                    print('unknown aggreagation function!!!')
                    exit(0)
        return table

    def print_table(self, table, q):
        '''print the resultant table in the required format'''
        # check if its inner join and remove the extra column if start is seen
        q.join_check()
        # process distinct
        tuples = self.process_distinct(table, q)
        
        for col_dict in q.columns:
            col = col_dict['table'] + '.' + col_dict['col']
            if 'function' in col_dict:
                col = col_dict['function'] + '(' + col + ')'
            print(col, end='\t')
        print()
        
        for tpl in tuples:
            for val in tpl:
                print(val, end='\t')
            print()

    def process_distinct(self, table, q):
        '''make the table have distinct rows'''
        tuples_list = []
        for i in range(len(list(table.values())[0])):
            temp = []
            for col_dict in q.columns:
                col = col_dict['table'] + '.' + col_dict['col']
                temp.append(table[col][i])
            if temp not in tuples_list:
                tuples_list.append(temp)
        return tuples_list

import os

with open('queries.txt') as fp:
    queries = fp.readlines()

queries = [query for query in queries if query is not '']

for query in queries:
    os.system(query)
    print('\n\n')

from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Create an index
index_name = 'employee_data'
es.indices.create(index=index_name, ignore=400)
print(f"Index '{index_name}' created.")

import csv

# Connect to Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Read the CSV file
with open('employee_sample_data.csv', mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Index each row to Elasticsearch
        es.index(index='employee_data', body=row)

print("Employee data indexed.")

# Verify data in Elasticsearch
res = es.search(index="employee_data", body={"query": {"match_all": {}}})
print("Search Results:", res['hits']['hits'])
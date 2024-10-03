from elasticsearch import Elasticsearch
import pandas as pd

# Connect to the Elasticsearch instance
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Create collection (index in Elasticsearch)
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Index {p_collection_name} created.")
    else:
        print(f"Index {p_collection_name} already exists.")

# Index employee data, excluding a specific column
def indexData(p_collection_name, p_exclude_column, sample_data):
    for index, employee in sample_data.iterrows():  # Correctly iterate over the DataFrame rows
        # Prepare the employee data for indexing
        employee_to_index = {
            key.replace(" ", "_"): (value if pd.notna(value) else None)
            for key, value in employee.items()
            if key != p_exclude_column
        }
        
        # Ensure all values are strings or None
        employee_to_index = {k: (str(v) if v is not None else None) for k, v in employee_to_index.items()}

        # Index the employee data
        try:
            es.index(index=p_collection_name, body=employee_to_index)
            print("Indexed Data:", employee_to_index)  # Show the data being indexed
        except Exception as e:
            print(f"Failed to index {employee_to_index}: {e}")

# Search for records by column and value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    query = {
        "query": {
            "match": {
                p_column_name: p_column_value
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    return results['hits']['hits']

# Get employee count in the collection
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)
    return count['count']

# Delete employee by ID
def delEmpById(p_collection_name, p_employee_id):
    # Example: Ensure you're targeting the right Employee_ID or conditions
    es.delete_by_query(index=p_collection_name, body={
        "query": {
            "match": {
                "Employee_ID": p_employee_id  # Use parameter for dynamic ID
            }
        }
    })

# Get department facet (group by department and count)
def getDepFacet(p_collection_name):
    query = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword"
                }
            }
        }
    }
    results = es.search(index=p_collection_name, body=query)
    return results['aggregations']['department_count']['buckets']

# Sample employee data to work with
sample_data = pd.read_csv("employee_sample_data.csv", encoding='ISO-8859-1')

# Function Execution as per assignment

# # Create collections
v_nameCollection = 'hash_jayaseelan'
v_phoneCollection = 'hash_3688'  

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Index data (excluding Department from v_nameCollection, and Gender from v_phoneCollection)
indexData(v_nameCollection, 'Department', sample_data)
indexData(v_phoneCollection, 'Gender', sample_data)

# Get employee count
print("Employee count in v_nameCollection:", getEmpCount(v_nameCollection))

# # Delete employee by ID
delEmpById(v_nameCollection, 'E02003')
print(f"Employee with ID E02003 deleted from collection '{v_nameCollection}'.")

# # Get employee count after deletion
print("Employee count after deletion in v_nameCollection:", getEmpCount(v_nameCollection))

# Search by column
print("Search by Department IT in v_nameCollection:", searchByColumn(v_nameCollection, 'Department', 'IT'))
print("Search by Gender Male in v_nameCollection:", searchByColumn(v_nameCollection, 'Gender', 'Male'))
print("Search by Department IT in v_phoneCollection:", searchByColumn(v_phoneCollection, 'Department', 'IT'))

# # Get department facet
print("Department facet in v_nameCollection:", getDepFacet(v_nameCollection))
print("Department facet in v_phoneCollection:", getDepFacet(v_phoneCollection))
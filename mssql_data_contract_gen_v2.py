import pyodbc
import re
import yaml
from collections import OrderedDict

# Replace these values with your MSSQL connection details
SERVER = 'server_name'
DATABASE = 'database_name'
USERNAME = 'username'
PASSWORD = 'password'
DRIVER = 'ODBC Driver 17 for SQL Server'

# Establish a connection
connection_string = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Dictionary of schema names and associated views (empty list implies all views)
# The following is a sample for the AdventureWorks database

# SCHEMA_VIEWS = {
#     'Person': [
#         'vStateProvinceCountryRegion'
#     ],
#     'HumanResources': [],
#     'Sales': []
# }

SCHEMA_VIEWS = {
    'Person': [
        'vStateProvinceCountryRegion'
    ],
    'HumanResources': [],
    'Sales': []
}

# Initialize the metadata dictionary
metadata = OrderedDict()

# Fetch view columns metadata
for schema, views in SCHEMA_VIEWS.items():
    # If views list is empty, fetch all views for the schema
    if not views:
        cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{schema}'")
        views = [row.TABLE_NAME for row in cursor.fetchall()]
    
    for view in views:
        try:
        
            # Get the DDL of the view
            cursor.execute(f"SELECT OBJECT_DEFINITION(OBJECT_ID('{schema}.{view}')) AS ViewDDL")
            view_ddl = cursor.fetchone().ViewDDL
            
            # Extract referenced tables from the DDL using regex
            if view_ddl is not None:
                referenced_tables = re.findall(fr'\[{schema}\]\.\[(.*?)\]', view_ddl)

                # Maintain a set of processed columns for each table
                processed_columns = {}

                # Fetch columns metadata for each referenced table
                for table in referenced_tables:
                    # Skip the view itself
                    if table == view:
                        continue

                    # Common Table Expression (CTE) to get the columns and data types from tables
                    query = f"""
                        WITH TableColumns AS (
                            SELECT
                                t.name AS TableName,
                                c.name AS ColumnName,
                                ty.name AS DataType,
                                CASE
                                    WHEN ty.name IN ('varchar', 'char', 'nvarchar', 'nchar') THEN c.max_length / 2
                                    ELSE c.max_length
                                END AS MaxLength,
                                ic.column_id AS IsPrimaryKey,
                                c.is_nullable AS IsNullable,
                                CAST(ep.value AS NVARCHAR(MAX)) AS ColumnDescription
                            FROM
                                sys.objects AS t
                            JOIN
                                sys.columns AS c ON t.object_id = c.object_id
                            JOIN
                                sys.types AS ty ON c.system_type_id = ty.system_type_id
                                                AND c.user_type_id = ty.user_type_id
                            LEFT JOIN
                                sys.index_columns AS ic ON t.object_id = ic.object_id
                                                    AND c.column_id = ic.column_id
                            LEFT JOIN
                                sys.extended_properties AS ep ON t.object_id = ep.major_id
                                                            AND c.column_id = ep.minor_id
                                                            AND ep.name = 'MS_Description'
                            WHERE
                                t.name = '{table}' AND t.schema_id = SCHEMA_ID('{schema}')
                        )
                        SELECT
                            TableName,
                            ColumnName,
                            DataType,
                            MaxLength,
                            IsPrimaryKey,
                            IsNullable,
                            ColumnDescription
                        FROM
                            TableColumns
                    """

                    print(f"Executing query: {query}")

                    cursor.execute(query)

                    # Print details of each row before fetchall
                    for column_info in cursor.description:
                        print(f"Column: {column_info[0]}, Type: {column_info[1]}")

                    rows = cursor.fetchall()

                    for row in rows:
                        table_name, column_name, data_type, max_length, is_primary_key, is_nullable, column_description = row

                        if view not in metadata:
                            metadata[view] = {'tables_referenced': {}}

                        if table_name not in metadata[view]['tables_referenced']:
                            metadata[view]['tables_referenced'][table_name] = {'description': None, 'columns': []}

                        # Check if the column has been processed for this table
                        if column_name not in processed_columns.get(table_name, set()):
                            metadata[view]['tables_referenced'][table_name]['columns'].append({
                                'column': column_name,
                                'isPrimaryKey': bool(is_primary_key),
                                'isNullable': bool(is_nullable),
                                'logicalType': data_type,
                                'physicalType': f"{data_type}({max_length})" if max_length else data_type,
                                'tags': None,
                                'description': column_description
                            })

                            # Add the column to the set of processed columns for this table
                            processed_columns.setdefault(table_name, set()).add(column_name)
                            
            else:
                print(f"View {schema}.{view} does not exist or DDL is empty. Skipping.")
                
        except pyodbc.Error as e:
                # Handle the error and continue to the next view
                print(f"Error processing view {schema}.{view}: {str(e)}")
                continue

# Close the cursor and connection
cursor.close()
connection.close()

# Define a custom representer to avoid !!python/object/apply tag
def ordered_dict_representer(dumper, data):
    return dumper.represent_dict(data.items())

# Add the custom representer to the YAML dumper
yaml.add_representer(OrderedDict, ordered_dict_representer)

# Save the metadata to a YAML file
metadata_output_path = 'output/mssql_metadata_output.yaml'
with open(metadata_output_path, 'w') as metadata_file:
    yaml.dump(metadata, metadata_file, default_flow_style=False)

print(f"Metadata saved to {metadata_output_path}")

# Now, incorporate the metadata into the main YAML structure
main_metadata = OrderedDict({
    "datasetDomain": None,
    "quantumName": None,
    "userConsumptionMode": None,
    "version": None,
    "status": None,
    "uuid": None,
    "description": {
        "purpose": None,
        "limitations": None,
        "usage": None
    },
    "tenant": None,
    "productDl": None,
    "productSlackChannel": None,
    "productFeedbackUrl": None,
    "sourcePlatform": None,
    "sourceSystem": None,
    "datasetProject": None,
    "datasetName": None,
    "kind": None,
    "apiVersion": None,
    "type": None,
    "driver": None,
    "driverVersion": None,
    "server": None,
    "database": None,
    "username": None,
    "password": None,
    "schedulerAppName": None,
    "dataset": [
        {
            "views": metadata
        }
    ],
    "price": {
        "priceAmount": None,
        "priceCurrency": None,
        "priceUnit": None
    },
    "stakeholders": [
        {
            "username": None,
            "role": None,
            "dateIn": None,
            "dateOut": None,
            "replacedByUsername": None
        },
    ],
    "roles": [
        {
            "role": None,
            "access": None,
            "firstLevelApprovers": None,
            "secondLevelApprovers": None
        },
    ],
    "slaDefaultColumn": None,
    "slaProperties": [
        {"property": None, "value": None, "unit": None, "column": None},
    ],
    "tags": None,
    "systemInstance": None,
    "contractCreatedTs": None
    })

# Save the final metadata to a YAML file
output_path = 'output/mssql_gen_data_contract_v2.yaml'
with open(output_path, 'w') as yaml_file:
    yaml.dump(main_metadata, yaml_file, default_flow_style=False, default_style=False)

print(f"View columns metadata saved to {output_path}")
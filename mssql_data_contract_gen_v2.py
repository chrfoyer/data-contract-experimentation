import pyodbc
import re
import yaml

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

# List of view names
VIEW_NAMES = []

# Initialize the metadata dictionary
metadata = {}

# Fetch view columns metadata
for view in VIEW_NAMES:
    # Get the DDL of the view
    cursor.execute(f"SELECT OBJECT_DEFINITION(OBJECT_ID('{view}')) AS ViewDDL")
    view_ddl = cursor.fetchone().ViewDDL

    # Extract referenced tables from the DDL using regex
    referenced_tables = re.findall(r'\[dbo\]\.\[(.*?)\]', view_ddl)

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
                    t.name = '{table}'
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

            metadata[view]['tables_referenced'][table_name]['columns'].append({
                'column': column_name,
                'isPrimaryKey': bool(is_primary_key),
                'isNullable': bool(is_nullable),
                'logicalType': data_type,
                'physicalType': f"{data_type}({max_length})" if max_length else data_type,
                'tags': None,
                'description': column_description
            })

# Close the cursor and connection
cursor.close()
connection.close()

# Save the metadata to a YAML file
output_path = 'view_columns_metadata_output.yaml'
with open(output_path, 'w') as yaml_file:
    yaml.dump(metadata, yaml_file, default_flow_style=False)

print(f"View columns metadata saved to {output_path}")
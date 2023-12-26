import pyodbc
import psycopg2
import yaml
import re

def extract_metadata_from_mssql(connection_string, object_name, source_schema, is_view=False):
    # Connect to MSSQL
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # Get column metadata
    object_type = 'VIEW' if is_view else 'BASE TABLE'

    if is_view:
        # Fetch view DDL dynamically
        cursor.execute(f"SET NOCOUNT ON; SELECT 'GO' + OBJECT_DEFINITION(object_id) FROM sys.views WHERE schema_id = SCHEMA_ID(N'{source_schema}') AND name = '{object_name}'")
        view_ddl_result = cursor.fetchone()

        if view_ddl_result:
            view_ddl = view_ddl_result[0]

            # Append view DDL to the file
            with open('view_ddl.txt', 'a') as view_file:
                view_file.write(view_ddl)
                view_file.write('\n\n')

            # Parse the view DDL to extract referenced columns and tables
            columns, tables = parse_view_ddl(view_ddl)
        else:
            columns, tables = [], []
    else:
        # For tables, get column metadata
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{object_name}' AND TABLE_SCHEMA = '{source_schema}'")
        columns = [{'name': row.COLUMN_NAME, 'type': row.DATA_TYPE} for row in cursor.fetchall()]
        tables = []

    # Close the connection
    conn.close()

    return columns, tables


def parse_view_ddl(view_ddl):
    columns = []
    tables = set()

    # Regular expression to extract column names
    column_pattern = re.compile(r'\[?([a-zA-Z_]\w*)\]?\s*(?:,|\n|$)', re.IGNORECASE)

    # Regular expression to extract table names
    table_pattern = re.compile(r'\bFROM\s+\[?([a-zA-Z_]\w*)\]?\s*(?:\b|$)', re.IGNORECASE)

    # Find all matches for columns
    column_matches = column_pattern.finditer(view_ddl)
    for match in column_matches:
        column_name = match.group(1)
        columns.append(column_name)

    # Find all matches for tables
    table_matches = table_pattern.finditer(view_ddl)
    for match in table_matches:
        table_name = match.group(1)
        tables.add(table_name)

    return columns, list(tables)

def extract_data_types_from_tables(connection_string, source_schema, tables):
    data_types = {}

    # Connect to MSSQL
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for table_name in tables:
        # Fetch column metadata for the table
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{source_schema}'")
        columns = {row.COLUMN_NAME: row.DATA_TYPE for row in cursor.fetchall()}
        data_types[table_name] = columns

    # Close the connection
    conn.close()

    return data_types

def extract_metadata_from_airflow(airflow_connection_string):
    metadata = {'dags': {}}

    # Connect to Airflow metadata database (Postgres)
    conn = psycopg2.connect(airflow_connection_string)
    cursor = conn.cursor()

    try:
        # Fetch DAG metadata
        cursor.execute("SELECT dag_id, is_active, schedule_interval FROM dag")
        dag_records = cursor.fetchall()

        for dag_record in dag_records:
            dag_id, is_active, schedule_interval = dag_record
            metadata['dags'][dag_id] = {'is_active': is_active, 'schedule_interval': schedule_interval}
            # metadata['dags'][dag_id] = {'is_active': is_active, 'schedule_interval': schedule_interval, 'human_readable_schedule': None}

            # # Parse CRON expression if available
            # if schedule_interval:
            #     try:
            #         yaml_output, human_readable_schedule = parse_cron_expression(schedule_interval)
            #         metadata['dags'][dag_id]['human_readable_schedule'] = human_readable_schedule
            #     except ValueError as ve:
            #         print(f"Error parsing CRON expression for {dag_id}: {ve}")
            #         metadata['dags'][dag_id]['human_readable_schedule'] = None
            # else:
            #     metadata['dags'][dag_id]['human_readable_schedule'] = None

    except Exception as e:
        print(f"Error fetching metadata from Airflow: {e}")
    finally:
        # Close the connection
        conn.close()

    return metadata

# def parse_cron_expression(cron_expression):
#     try:
#         # Handle special cases
#         if cron_expression.lower() == 'null':
#             return '', ''
#         elif cron_expression.startswith('@'):
#             frequency = cron_expression[1:].lower()
#             human_readable_schedule = f"{frequency.capitalize()}"
#             return '', human_readable_schedule

#         # Clean up the string by removing leading/trailing spaces and quotes
#         cleaned_expression = cron_expression.strip('"').strip()

#         # Check if the expression is a specific time or ID (e.g., "0 5 * * *" or "*id001")
#         if cleaned_expression.startswith('*id'):
#             human_readable_schedule = cleaned_expression
#             return '', human_readable_schedule

#         cron_parts = cleaned_expression.split()

#         # Check if the expression is a specific time (e.g., "0 5 * * *")
#         if len(cron_parts) == 5 and cron_parts[0] == '0':
#             hour = cron_parts[1]
#             human_readable_schedule = f"Every {cron_parts[2]} hours"
#             return '', human_readable_schedule

#         # Extracting relevant parts for a basic human-readable schedule
#         minute = cron_parts[0] if len(cron_parts) > 0 else '*'
#         hour = cron_parts[1] if len(cron_parts) > 1 else '*'
#         day_of_month = cron_parts[2] if len(cron_parts) > 2 else '*'
#         month = cron_parts[3] if len(cron_parts) > 3 else '*'
#         day_of_week = cron_parts[4] if len(cron_parts) > 4 else '*'

#         human_readable_schedule = (
#             f"At {minute} minutes, at {hour} hours, on day {day_of_month} of month, in {month}, on {day_of_week}"
#         )

#         return '', human_readable_schedule
#     except ValueError as ve:
#         raise ve  # Reraise the exception for specific handling
#     except Exception as e:
#         print(f"Error parsing CRON expression: {e}")
#         return None, None
    
def extract_table_names_from_postgres(destination_connection_string, destination_schema):
    table_names = []

    # Connect to Postgres
    conn = psycopg2.connect(destination_connection_string)
    cursor = conn.cursor()

    try:
        # Get list of tables in the specified schema
        formatted_schema = f"'{destination_schema}'"  # Format the schema name with single quotes
        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = {formatted_schema}")
        table_names = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching table names from Postgres: {e}")
    finally:
        # Close the connection
        conn.close()

    return table_names


def extract_metadata_from_postgres(destination_connection_string, destination_schema, table_name):
    columns = []

    # Connect to Postgres
    conn = psycopg2.connect(destination_connection_string)
    cursor = conn.cursor()

    try:
        # Get column metadata
        formatted_table_name = f"'{table_name}'"  # Format the table name
        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = {formatted_table_name} AND table_schema = '{destination_schema}'")
        columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching metadata from Postgres: {e}")
    finally:
        # Close the connection
        conn.close()

    return columns


def generate_yaml_from_ddl(source_connection_string, destination_connection_string, airflow_connection_string,
                           source_schema, destination_schema, source_tables, source_views):
    data_contract = {
        'source': {
            'tables': {},
            'views': {},
        },
        'destination': {},
        'airflow': {'dags': {}},
    }

    # Extract metadata for source tables
    for table_name in source_tables:
        columns, _ = extract_metadata_from_mssql(source_connection_string, table_name, source_schema, is_view=False)
        data_contract['source']['tables'][table_name] = {'columns': columns}

    # Extract metadata for source views
    for view_name in source_views:
        columns, tables = extract_metadata_from_mssql(source_connection_string, view_name, source_schema, is_view=True)

        # Extract data types from referenced tables
        data_types = extract_data_types_from_tables(source_connection_string, source_schema, tables)

        # Use data types to populate the columns in the data contract
        columns_with_types = []

        for col_info in columns:
            if isinstance(col_info, dict):
                col_name = col_info['name']
                col_type = 'UNKNOWN'

                for table in tables:
                    if table in data_types and col_name in data_types[table]:
                        col_type = data_types[table][col_name]
                        break

                columns_with_types.append({'name': col_name, 'type': col_type})

        data_contract['source']['views'][view_name] = {'columns': columns_with_types, 'referenced_tables': tables}

    # Extract metadata from Airflow
    airflow_metadata = extract_metadata_from_airflow(airflow_connection_string)
    for dag_id, dag_info in airflow_metadata['dags'].items():
        data_contract['airflow']['dags'][dag_id] = {
            'is_active': dag_info['is_active'],
            'schedule_interval': dag_info['schedule_interval'],
            # 'human_readable_schedule': dag_info['human_readable_schedule'],
        }

    # Extract metadata from Postgres (destination)
    for table_name in extract_table_names_from_postgres(destination_connection_string, destination_schema):
        columns = extract_metadata_from_postgres(destination_connection_string, destination_schema, table_name)
        data_contract['destination'][table_name] = {'columns': columns}

    return data_contract

def save_yaml(data_contract, yaml_file_path):
    with open(yaml_file_path, 'w') as yaml_file:
        yaml.dump(data_contract, yaml_file, default_flow_style=False)
     
# Replace the placeholders with your actual SQL Server and Postgres connection details
source_server = 'server_address'
source_database = 'server_database'
source_schema = 'server_schema'
source_username = 'server_username'
source_password = 'server_password'

destination_server = 'destination_server_address'
destination_database = 'destination_database_name'
destination_username = 'destination_username'
destination_password = 'destination_password'
destination_schema = 'destination_schema'

airflow_server = 'airflow_server_address'
airflow_database = 'airflow'
airflow_username = 'airflow'
airflow_password = 'airflow'
   
# Create the MSSQL connection string for source
source_connection_string = f'DRIVER={{SQL Server}};SERVER={source_server};DATABASE={source_database};UID={source_username};PWD={source_password}'

# Create the Postgres connection string for destination
# Adjust the connection string based on your Postgres setup
destination_connection_string = f"host={destination_server} dbname={destination_database} user={destination_username} password={destination_password}"

# Create the Airflow connection string
# Adjust the connection string based on your Airflow database setup
airflow_connection_string = f"host={airflow_server} dbname={airflow_database} user={airflow_username} password={airflow_password}"

# Specify the source tables and views
source_tables = []

source_views = []

# Specify the full path for the YAML file
yaml_file_path = '/path/to/yaml_file.yaml'

# Generate and save the YAML data contract
yaml_data_contract = generate_yaml_from_ddl(
    source_connection_string,
    destination_connection_string,
    airflow_connection_string,
    source_schema,
    destination_schema,
    source_tables,
    source_views
)
save_yaml(yaml_data_contract, yaml_file_path)
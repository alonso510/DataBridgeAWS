import boto3
import json
import os
import csv
from io import StringIO, BytesIO
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, List, Any, Tuple

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def clean_column_name(column: str) -> str:
    """Make column names Redshift-friendly."""
    if not column:
        return "unnamed_column"
    cleaned = ''.join(c if c.isalnum() else '_' for c in str(column).lower())
    if cleaned[0].isdigit():
        cleaned = 'col_' + cleaned
    return cleaned

def detect_data_type(values: List[Any]) -> str:
    """Detect the most appropriate Redshift data type based on column values."""
    has_number = False
    has_decimal = False
    has_date = False
    max_length = 0

    for val in values:
        if val is None or val == '':
            continue
        if isinstance(val, datetime):
            has_date = True
        if isinstance(val, (int, float)):
            has_number = True
            if isinstance(val, float):
                has_decimal = True
        if isinstance(val, str):
            max_length = max(max_length, len(val))
            try:
                datetime.strptime(val, '%Y-%m-%d')
                has_date = True
            except:
                pass

    if has_date:
        return 'TIMESTAMP'
    elif has_decimal:
        return 'DECIMAL(18,2)'
    elif has_number:
        return 'BIGINT'
    else:
        length = max(max_length * 2, 100)
        length = min(length, 65535)
        return f'VARCHAR({length})'

def load_workbook_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Load a workbook from S3 using pandas."""
    try:
        logger.info(f"Loading workbook from bucket: {bucket}, key: {key}")
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        print(file_content)
        logger.info(f"Read {len(file_content)} bytes from S3.")

        if len(file_content) == 0:
            raise ValueError("The file is empty.")

        # Load the workbook using pandas
        df = pd.read_excel('s3://adventureworks-dw-an/raw/' + key, engine='openpyxl') # Load all sheets
        logger.info(f"Workbook loaded successfully.")
        return df
    except Exception as e:
        logger.error(f"Error loading workbook from S3: {str(e)}")
        raise

def create_table_if_not_exists(redshift_data: boto3.client, table_name: str, headers: List[str], column_types: Dict[str, str]) -> None:
    """Create table in Redshift if it doesn't exist."""
    try:
        columns = [f"{header} {column_types[header]}" for header in headers]
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {','.join(columns)}
        );
        """
        logger.info(f"Creating table with SQL: {create_table_sql}")
        response = redshift_data.execute_statement(
            Database=os.environ['REDSHIFT_DATABASE'],
            WorkgroupName=os.environ['REDSHIFT_WORKGROUP'],
            Sql=create_table_sql
        )
        logger.info(f"Created table {table_name} with response: {response}")
    except Exception as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def process_worksheet(df: pd.DataFrame, sheet_name: str) -> Tuple[List[str], List[Dict[str, Any]], Dict[str, str]]:
    """Process a single worksheet and return headers, data, and column types."""
    data = df.to_dict('records')
    headers = list(df.columns)
    headers = [clean_column_name(header) for header in headers]
    column_types = {header: detect_data_type(df[header].tolist()) for header in headers}
    return headers, data, column_types

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file from bucket: {bucket}, key: {key}")

        df = load_workbook_from_s3(bucket, key)
        redshift_data = boto3.client('redshift-data')

        for sheet_name, df_sheet in df.items(): 
            headers, data, column_types = process_worksheet(df_sheet, sheet_name)
            base_table_name = clean_column_name(os.path.splitext(os.path.basename(key))[0])
            table_name = f"{base_table_name}_{clean_column_name(sheet_name)}"
            create_table_if_not_exists(redshift_data, table_name, headers, column_types)

            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

            copy_command = f"""
            COPY {table_name}
            FROM STDOUT
            IAM_ROLE '{os.environ['REDSHIFT_ROLE_ARN']}'
            CSV HEADER;
            """

            # Use the CSV buffer directly in the copy command
            response = redshift_data.execute_statement(
                Database=os.environ['REDSHIFT_DATABASE'],
                WorkgroupName=os.environ['REDSHIFT_WORKGROUP'],
                Sql=copy_command,
                StatementArguments=[csv_buffer.getvalue().encode('utf-8')]
            )

            logger.info(f"Copy command result: {response}")

        return {'statusCode': 200, 'body': json.dumps(f'Successfully processed all worksheets from {key}')}
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error processing file: {str(e)}')}
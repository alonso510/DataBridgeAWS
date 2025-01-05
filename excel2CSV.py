import boto3
import pandas as pd
from io import BytesIO
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def clean_sheet_name(name: str) -> str:
    """Clean sheet name for use in file naming"""
    return ''.join(c if c.isalnum() else '_' for c in str(name).lower())

def process_excel_to_csvs(excel_file: BytesIO, filename: str, s3_client, bucket: str) -> list:
    """
    Process all sheets in an Excel file and convert each to CSV
    Returns list of created CSV files
    """
    created_files = []
    
    # Get all sheet names
    xlsx = pd.ExcelFile(excel_file)
    logger.info(f"Found sheets: {xlsx.sheet_names}")
    
    for sheet_name in xlsx.sheet_names:
        try:
            logger.info(f"Processing sheet: {sheet_name}")
            
            # Read the sheet
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            logger.info(f"Sheet {sheet_name} has {len(df)} rows and {len(df.columns)} columns")
            
            # Generate CSV filename
            base_name = os.path.splitext(filename)[0]
            clean_sheet = clean_sheet_name(sheet_name)
            csv_key = f"processed/{base_name}_{clean_sheet}.csv"
            
            # Convert to CSV and upload
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket,
                Key=csv_key,
                Body=csv_buffer.getvalue()
            )
            
            created_files.append(csv_key)
            logger.info(f"Successfully created {csv_key}")
            
        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {str(e)}")
            continue
    
    return created_files

def move_to_archived(s3_client, bucket: str, key: str):
    """Move original Excel file to archived folder"""
    try:
        # Generate new key for archived folder
        filename = os.path.basename(key)
        archived_key = f"archived/{filename}"
        
        # Copy file to archived folder
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=archived_key
        )
        
        # Delete from original location
        s3_client.delete_object(Bucket=bucket, Key=key)
        
        logger.info(f"Moved {key} to {archived_key}")
        
    except Exception as e:
        logger.error(f"Error archiving file: {str(e)}")
        raise

def lambda_handler(event, context):
    try:
        # Get S3 details
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        filename = os.path.basename(key)
        
        logger.info(f"Processing {filename} from {bucket}")
        
        # Initialize S3
        s3 = boto3.client('s3')
        
        # Read Excel file
        response = s3.get_object(Bucket=bucket, Key=key)
        excel_file = BytesIO(response['Body'].read())
        
        # Process all sheets
        created_files = process_excel_to_csvs(excel_file, filename, s3, bucket)
        
        # Move original file to archived folder
        move_to_archived(s3, bucket, key)
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully processed Excel file',
                'source_file': key,
                'created_files': created_files
            }
        }
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing file: {str(e)}'
        }
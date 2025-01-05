import boto3
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # S3 details
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Attempting to process {key} from {bucket}")
        
        # Initialize S3
        s3 = boto3.client('s3')
        
        # Read Excel file
        response = s3.get_object(Bucket=bucket, Key=key)
        excel_file = BytesIO(response['Body'].read())
        
        # Read first sheet using pandas
        df = pd.read_excel(excel_file, sheet_name=0)
        logger.info(f"Successfully read sheet with {len(df)} rows")
        
        # Convert to CSV string
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload to S3
        output_key = f"processed/{key.split('/')[-1].replace('.xlsx', '_sheet1.csv')}"
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue()
        )
        
        logger.info(f"Successfully saved CSV to {output_key}")
        
        return {
            'statusCode': 200,
            'body': f'Successfully converted first sheet to CSV: {output_key}'
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise
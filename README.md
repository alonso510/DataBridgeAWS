# AWS Excel-to-Redshift ETL Pipeline 🚀

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)

## Overview 📋
Engineered an automated ETL pipeline using AWS Lambda, S3, and Redshift Serverless that dynamically processes multi-sheet Excel workbooks into a scalable data warehouse solution. The system features automated schema detection, intelligent error handling, and comprehensive logging, transforming complex Excel data structures into optimized database tables while maintaining data integrity throughout the entire pipeline.

## Architecture 🏗️
```mermaid
graph LR
    A[Excel Files] --> B[S3 Bucket/raw]
    B --> C[Lambda 1: Excel to CSV]
    C --> D[S3 Bucket/processed]
    D --> E[Lambda 2: CSV to Redshift]
    E --> F[Redshift Serverless]
```

## Features ✨
- **Dynamic Schema Inference**: Automatically detects and creates appropriate database schemas
- **Multi-Sheet Processing**: Handles multiple worksheets within Excel workbooks
- **Intelligent Wait Mechanism**: Ensures all Excel to CSV conversions complete before database loading
- **Error Handling**: Comprehensive error management and logging
- **Data Type Detection**: Automatic detection and mapping of data types
- **File Management**: Automated file organization across processing stages

## Tech Stack 💻
- **AWS Services**:
  - Lambda Functions
  - S3 Bucket Storage
  - Redshift Serverless
  - CloudWatch Logs
  - IAM Roles & Policies
- **Languages & Libraries**:
  - Python 3.9
  - Pandas
  - AWS SDK (boto3)
  - openpyxl

## Setup and Configuration 🛠️

### Prerequisites
```bash
# Required Python packages
pandas==1.5.3
openpyxl==3.1.2
```

### S3 Bucket Structure
```plaintext
bucket-name/
├── raw/         # Original Excel files
├── processed/   # Converted CSV files
└── completed/   # Processed CSV files
```

### Lambda Configuration
1. **Excel to CSV Lambda**:
```python
Runtime: Python 3.9
Memory: 1024 MB
Timeout: 5 minutes
```

2. **CSV to Redshift Lambda**:
```python
Runtime: Python 3.9
Memory: 1024 MB
Timeout: 10 minutes
```

## IAM Permissions Required 🔒
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::bucket-name",
                "arn:aws:s3:::bucket-name/*"
            ]
        }
    ]
}
```

## Monitoring and Logging 📊
- CloudWatch Logs for both Lambda functions
- S3 event notifications
- Redshift query monitoring

## Error Handling 🚨
- File processing validation
- Data type conversion errors
- Schema mismatch handling
- Network timeout management

## Future Improvements 🔮
- [ ] Add data validation rules
- [ ] Implement parallel processing
- [ ] Add data quality checks
- [ ] Create monitoring dashboard

## Contributing 🤝
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License 📝
[MIT](https://choosealicense.com/licenses/mit/)

---

## Contact 📬
Your Name - [Your LinkedIn](https://www.linkedin.com/in/jose-nunez-444aa1232/)

Project Link: [https://github.com/alonso510/DataBridgeAWS](https://github.com/alonso510/DataBridgeAWS)

---
*Built with ❤️ using AWS Serverless*

# FTP Download Script with Optional S3 Upload

A robust Python script for downloading files from FTP servers based on a CSV file list, with optional S3 upload capability. Perfect for data engineering workflows, ETL pipelines, and AWS CloudShell environments.

## Features

- **Dual Operation Modes**: Local download or FTP-to-S3 transfer
- **CSV-driven**: File list managed through CSV files
- **Comprehensive Error Handling**: Detailed logging and failure tracking
- **AWS Integration**: Seamless CloudShell and IAM role support
- **Resource Management**: Smart temporary storage handling
- **Production Ready**: Extensive logging, progress tracking, and summary reports

## Requirements

### Python Dependencies
```bash
pip install boto3  # Only required for S3 functionality
```

### AWS Permissions (S3 Mode Only)
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject", 
                "s3:HeadObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Usage

### Basic Syntax
```bash
python ftp_script.py <csv_file> --ftp-host <host> --ftp-user <user> --ftp-password <password> [options]
```

### Operation Modes

#### Local Download Mode
Downloads files to local directory (default behavior):
```bash
python ftp_script.py filenames.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass
```

#### S3 Upload Mode
Downloads from FTP and uploads to S3 (enabled when `--s3-bucket` is provided):
```bash
python ftp_script.py filenames.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-data-bucket
```

## Command Line Arguments

### Required Arguments
| Argument | Description |
|----------|-------------|
| `csv_file` | Path to CSV file containing filenames |
| `--ftp-host` | FTP server hostname or IP address |
| `--ftp-user` | FTP username |
| `--ftp-password` | FTP password |

### S3 Arguments (Optional - enables S3 mode)
| Argument | Default | Description |
|----------|---------|-------------|
| `--s3-bucket` | None | S3 bucket name (enables S3 upload mode) |
| `--s3-prefix` | `""` | S3 key prefix/folder path |
| `--aws-region` | Default | AWS region for S3 operations |
| `--overwrite-s3` | False | Overwrite existing files in S3 |

### FTP Arguments (Optional)
| Argument | Default | Description |
|----------|---------|-------------|
| `--ftp-port` | `21` | FTP server port |
| `--remote-path` | `"/get"` | Remote FTP directory path |

### Local Storage Arguments (Optional)
| Argument | Default | Description |
|----------|---------|-------------|
| `--local-path` | `"./downloads"` | Local download directory (local mode only) |

### Processing Arguments (Optional)
| Argument | Default | Description |
|----------|---------|-------------|
| `--filename-column` | `"filename"` | CSV column name containing filenames |
| `--no-delete` | False | Don't delete files from FTP after download |

## CSV File Format

The CSV file should contain a column with filenames. Default column name is `filename`, but can be customized:

```csv
filename,description,size
file1.txt,Customer data,1024
file2.csv,Sales report,2048
file3.pdf,Monthly summary,512
```

Alternative column name example:
```csv
file_name,type,priority
data1.txt,csv,high
data2.json,json,medium
```
*Use with `--filename-column file_name`*

## Usage Examples

### Basic Examples

**1. Simple FTP download to local directory:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass
```

**2. Download to specific local directory:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --local-path /home/user/data
```

**3. Download without deleting from FTP:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --no-delete
```

### S3 Examples

**4. Basic FTP to S3 transfer:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-data-bucket
```

**5. S3 with folder organization:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-data-bucket \
  --s3-prefix "incoming/$(date +%Y/%m/%d)"
```

**6. S3 with overwrite enabled:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-data-bucket \
  --overwrite-s3
```

### Advanced Examples

**7. Custom FTP port and remote directory:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-port 2121 \
  --ftp-user myuser \
  --ftp-password mypass \
  --remote-path /data/exports \
  --s3-bucket my-bucket \
  --s3-prefix "external-data/"
```

**8. Custom CSV column name:**
```bash
python ftp_script.py data_files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --filename-column file_path \
  --s3-bucket my-bucket
```

**9. AWS CloudShell with specific region:**
```bash
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-bucket \
  --aws-region us-west-2 \
  --s3-prefix "data/$(date +%Y-%m-%d)/"
```

**10. Production ETL pipeline example:**
```bash
python ftp_script.py daily_files.csv \
  --ftp-host secure-ftp.vendor.com \
  --ftp-port 990 \
  --ftp-user data_user \
  --ftp-password $FTP_PASSWORD \
  --remote-path /daily_exports \
  --s3-bucket company-data-lake \
  --s3-prefix "raw/vendor-data/$(date +%Y/%m/%d)/" \
  --aws-region us-east-1 \
  --filename-column export_filename
```

## Environment-Specific Usage

### AWS CloudShell
Perfect for CloudShell environments with built-in AWS credentials:
```bash
# No AWS credentials needed - uses CloudShell IAM role
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --s3-bucket my-bucket
```

### Local Development
For local testing and development:
```bash
# Downloads to local directory for inspection
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password mypass \
  --local-path ./test_downloads \
  --no-delete
```

### CI/CD Pipelines
For automated pipelines with environment variables:
```bash
python ftp_script.py $CSV_FILE \
  --ftp-host $FTP_HOST \
  --ftp-user $FTP_USER \
  --ftp-password $FTP_PASSWORD \
  --s3-bucket $S3_BUCKET \
  --s3-prefix "automated/$(date +%Y%m%d_%H%M%S)/"
```

## Output and Logging

### Console Output
The script provides real-time progress updates and a comprehensive summary:

```
2025-08-03 10:30:15 - INFO - Successfully connected to FTP server ftp.example.com
2025-08-03 10:30:15 - INFO - Successfully connected to S3 bucket: my-data-bucket
2025-08-03 10:30:16 - INFO - Processing: file1.txt
2025-08-03 10:30:17 - INFO - Downloaded to temp: file1.txt
2025-08-03 10:30:18 - INFO - Uploaded to S3: file1.txt
2025-08-03 10:30:18 - INFO - Deleted from FTP: file1.txt

============================================================
FTP TO S3 TRANSFER SUMMARY
============================================================
Files successfully transferred to S3: 3
Files not found on FTP: 0
FTP download failures: 0
S3 upload failures: 0
FTP deletion failures: 0
Files already in S3 (skipped): 1

Successfully transferred: file1.txt, file2.csv, file3.pdf
```

### Log Files
Detailed logs are written to `ftp_to_s3.log` (S3 mode) or `ftp_operations.log` (local mode).

## Error Handling

The script handles various error scenarios:

- **FTP Connection Issues**: Network timeouts, authentication failures
- **File Not Found**: Missing files on FTP server
- **S3 Access Issues**: Permission errors, bucket not found
- **Disk Space**: Temporary storage limitations
- **Network Interruptions**: Graceful retry and cleanup

### Exit Codes
- `0`: Success - all files processed successfully
- `1`: Failure - one or more files failed to process

## Best Practices

### Security
```bash
# Use environment variables for sensitive data
export FTP_PASSWORD="your_secure_password"
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password $FTP_PASSWORD \
  --s3-bucket my-bucket
```

### Production Workflows
```bash
# Add error handling and notifications
python ftp_script.py files.csv \
  --ftp-host ftp.example.com \
  --ftp-user myuser \
  --ftp-password $FTP_PASSWORD \
  --s3-bucket my-bucket \
  --s3-prefix "ingestion/$(date +%Y%m%d)/" || {
    echo "FTP transfer failed" | mail -s "ETL Alert" admin@company.com
    exit 1
}
```

### Data Organization
```bash
# Organize data by date and source
python ftp_script.py vendor_files.csv \
  --ftp-host vendor-ftp.com \
  --ftp-user datauser \
  --ftp-password $VENDOR_PASSWORD \
  --s3-bucket data-lake \
  --s3-prefix "raw/vendor-name/$(date +%Y/%m/%d)/"
```

## Troubleshooting

### Common Issues

**1. S3 Permission Denied**
```bash
# Verify bucket access
aws s3 ls s3://your-bucket-name

# Check IAM permissions
aws sts get-caller-identity
```

**2. FTP Connection Timeout**
```bash
# Test FTP connectivity
telnet ftp.example.com 21

# Try different port if needed
python ftp_script.py ... --ftp-port 2121
```

**3. CSV Column Not Found**
```bash
# Check CSV headers
head -1 your_file.csv

# Specify correct column name
python ftp_script.py ... --filename-column your_column_name
```

**4. Files Already Exist in S3**
```bash
# Enable overwrite mode
python ftp_script.py ... --overwrite-s3

# Or check existing files
aws s3 ls s3://bucket/prefix/
```

### Debug Mode
Enable detailed logging by modifying the script's logging level:
```python
# Change in script
logging.basicConfig(level=logging.DEBUG, ...)
```

## Integration Examples

### Cron Job
```bash
# Daily at 2 AM
0 2 * * * /usr/bin/python3 /path/to/ftp_script.py /path/to/files.csv --ftp-host ftp.example.com --ftp-user user --ftp-password $FTP_PASS --s3-bucket daily-data
```

### Airflow DAG
```python
from airflow.operators.bash import BashOperator

download_task = BashOperator(
    task_id='ftp_to_s3',
    bash_command='''
    python /path/to/ftp_script.py {{ ds }}_files.csv \
      --ftp-host ftp.example.com \
      --ftp-user {{ var.value.ftp_user }} \
      --ftp-password {{ var.value.ftp_password }} \
      --s3-bucket {{ var.value.s3_bucket }} \
      --s3-prefix "data/{{ ds }}/"
    '''
)
```

### AWS Lambda
Package the script for serverless execution with appropriate IAM roles and VPC configuration for FTP access.

## Support

For issues and questions:
1. Check the log files for detailed error messages
2. Verify FTP and S3 connectivity independently  
3. Test with a small subset of files first
4. Ensure proper IAM permissions for S3 operations
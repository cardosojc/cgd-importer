#!/usr/bin/env python3
"""
FTP Download Script with Optional S3 Upload
Downloads files from FTP based on CSV list with optional S3 transfer capability
Works for both local downloads and AWS CloudShell S3 workflows
"""

import csv
import os
import logging
import tempfile
from pathlib import Path
from typing import List, Optional
import argparse
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import paramiko
from paramiko import SSHClient, SFTPClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ftp_to_s3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SFTPManager:
    """Handles SFTP operations for downloading and deleting files"""

    def __init__(self, host: str, username: str, password: str, port: int = 22):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.ssh = None
        self.sftp = None

    def connect(self) -> bool:
        """Establish SFTP connection"""
        try:
            self.ssh = SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.host, port=self.port, username=self.username, password=self.password)
            self.sftp = self.ssh.open_sftp()
            logger.info(f"Successfully connected to SFTP server {self.host}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {e}")
            return False

    def disconnect(self):
        """Close SFTP connection"""
        if self.sftp:
            try:
                self.sftp.close()
                logger.info("SFTP connection closed")
            except:
                pass
        if self.ssh:
            try:
                self.ssh.close()
            except:
                pass

    def change_directory(self, remote_path: str) -> bool:
        """Change to remote directory"""
        try:
            self.sftp.chdir(remote_path)
            logger.info(f"Changed to directory: {remote_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to change directory to {remote_path}: {e}")
            return False

    def file_exists(self, filename: str) -> bool:
        """Check if file exists on SFTP server"""
        try:
            self.sftp.stat(filename)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Error checking if file exists: {e}")
            return False

    def download_file_to_local(self, remote_filename: str, local_path: str) -> Optional[str]:
        """Download file from SFTP server to local directory"""
        try:
            local_filepath = Path(local_path) / remote_filename

            # Create local directory if it doesn't exist
            local_filepath.parent.mkdir(parents=True, exist_ok=True)

            # Get file size for progress tracking
            try:
                file_stat = self.sftp.stat(remote_filename)
                file_size = file_stat.st_size
            except:
                file_size = 0

            # Progress callback
            def progress_callback(transferred, total):
                if total > 0:
                    percentage = (transferred / total) * 100
                    print(f"\r{remote_filename}: {percentage:.1f}% ({transferred}/{total} bytes)", end='', flush=True)

            self.sftp.get(remote_filename, str(local_filepath), callback=progress_callback)
            
            # New line after progress
            print()
            logger.info(f"Downloaded: {remote_filename} -> {local_filepath}")
            return str(local_filepath)

        except Exception as e:
            print()  # Ensure we're on a new line after any progress output
            logger.error(f"Failed to download {remote_filename}: {e}")
            return None

    def download_file_to_temp(self, remote_filename: str, temp_dir: str) -> Optional[str]:
        """Download file from SFTP server to temporary location"""
        try:
            temp_filepath = os.path.join(temp_dir, remote_filename)

            # Get file size for progress tracking
            try:
                file_stat = self.sftp.stat(remote_filename)
                file_size = file_stat.st_size
            except:
                file_size = 0

            # Progress callback
            def progress_callback(transferred, total):
                if total > 0:
                    percentage = (transferred / total) * 100
                    print(f"\r{remote_filename}: {percentage:.1f}% ({transferred}/{total} bytes)", end='', flush=True)

            self.sftp.get(remote_filename, temp_filepath, callback=progress_callback)
            
            # New line after progress
            print()
            logger.info(f"Downloaded to temp: {remote_filename}")
            return temp_filepath

        except Exception as e:
            print()  # Ensure we're on a new line after any progress output
            logger.error(f"Failed to download {remote_filename}: {e}")
            return None

    def delete_file(self, filename: str) -> bool:
        """Delete file from SFTP server"""
        try:
            self.sftp.remove(filename)
            logger.info(f"Deleted from SFTP: {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {filename}: {e}")
            return False

    def list_files(self) -> List[str]:
        """List all files in current SFTP directory"""
        try:
            file_list = self.sftp.listdir()
            # Filter out directories by checking file attributes
            files = []
            for item in file_list:
                try:
                    stat = self.sftp.stat(item)
                    import stat as stat_module
                    if not stat_module.S_ISDIR(stat.st_mode):
                        files.append(item)
                except:
                    # If we can't stat, assume it's a file if it has an extension
                    if '.' in item:
                        files.append(item)
            logger.info(f"Found {len(files)} files in current directory")
            return files
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []


class S3Manager:
    """Handles S3 operations for uploading files"""

    def __init__(self, bucket_name: str, aws_region: str = None):
        self.bucket_name = bucket_name
        self.aws_region = aws_region

        try:
            # Initialize S3 client - will use IAM role in CloudShell
            if aws_region:
                self.s3_client = boto3.client('s3', region_name=aws_region)
            else:
                self.s3_client = boto3.client('s3')

            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {bucket_name}")

        except NoCredentialsError:
            logger.error("AWS credentials not found. Ensure IAM role is configured in CloudShell.")
            raise
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{bucket_name}' not found")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket '{bucket_name}'")
            else:
                logger.error(f"Error accessing S3 bucket: {e}")
            raise

    def upload_file(self, local_file_path: str, s3_key: str,
                    extra_args: dict = None) -> bool:
        """Upload file to S3 bucket"""
        try:
            if extra_args is None:
                extra_args = {}

            self.s3_client.upload_file(local_file_path, self.bucket_name, s3_key,
                                       ExtraArgs=extra_args)
            logger.info(f"Uploaded to S3: {s3_key}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload {local_file_path} to S3: {e}")
            return False

    def file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3 bucket"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking S3 object existence: {e}")
                return False


def read_csv_filenames(csv_path: str, filename_column: str = 'filename') -> Optional[List[str]]:
    """Read filenames from CSV file. Returns None if file doesn't exist."""
    filenames = []

    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            if filename_column not in reader.fieldnames:
                logger.error(f"Column '{filename_column}' not found in CSV. Available columns: {reader.fieldnames}")
                return []

            for row in reader:
                filename = row[filename_column].strip()
                if filename:  # Skip empty filenames
                    filenames.append(filename)

        logger.info(f"Read {len(filenames)} filenames from {csv_path}")
        return filenames

    except FileNotFoundError:
        logger.warning(f"CSV file {csv_path} not found")
        return None
    except Exception as e:
        logger.error(f"Failed to read CSV file {csv_path}: {e}")
        return []


def process_files(sftp_manager: SFTPManager, s3_manager: Optional[S3Manager],
                  filenames: List[str], remote_sftp_path: str = "/",
                  local_download_path: str = "./downloads", s3_prefix: str = "",
                  delete_from_sftp: bool = True, overwrite_s3: bool = False) -> dict:
    """Process files: SFTP download -> optional S3 upload -> cleanup"""

    results = {
        'success': [],
        'sftp_not_found': [],
        'sftp_download_failed': [],
        's3_upload_failed': [],
        'sftp_delete_failed': [],
        's3_already_exists': []
    }

    # Change to remote directory
    if not sftp_manager.change_directory(remote_sftp_path):
        logger.error(f"Cannot proceed - failed to access remote directory: {remote_sftp_path}")
        return results

    # Determine download strategy
    use_s3 = s3_manager is not None
    download_dir = tempfile.mkdtemp(prefix='ftp_download_') if use_s3 else local_download_path

    # Create local directory if not using S3 and directory doesn't exist
    if not use_s3:
        Path(local_download_path).mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading to local directory: {local_download_path}")
    else:
        logger.info(f"Using temporary directory for S3 upload: {download_dir}")

    try:
        for filename in filenames:
            logger.info(f"Processing: {filename}")

            # Check if file exists on SFTP
            if not sftp_manager.file_exists(filename):
                logger.warning(f"File not found on SFTP server: {filename}")
                results['sftp_not_found'].append(filename)
                continue

            # S3-specific checks
            if use_s3:
                s3_key = f"{s3_prefix.rstrip('/')}/{filename}" if s3_prefix else filename

                # Check if file already exists in S3
                if not overwrite_s3 and s3_manager.file_exists(s3_key):
                    logger.warning(f"File already exists in S3: {s3_key}")
                    results['s3_already_exists'].append(filename)
                    continue

            # Download from SFTP
            if use_s3:
                file_path = sftp_manager.download_file_to_temp(filename, download_dir)
            else:
                file_path = sftp_manager.download_file_to_local(filename, download_dir)

            if not file_path:
                results['sftp_download_failed'].append(filename)
                continue

            # Handle S3 upload or mark local download as success
            if use_s3:
                try:
                    # Upload to S3
                    if s3_manager.upload_file(file_path, s3_key):
                        upload_success = True
                    else:
                        results['s3_upload_failed'].append(filename)
                        upload_success = False

                    # Clean up temp file
                    try:
                        os.remove(file_path)
                    except:
                        pass

                except Exception as e:
                    logger.error(f"Error during S3 upload process for {filename}: {e}")
                    results['s3_upload_failed'].append(filename)
                    upload_success = False

                if not upload_success:
                    continue

            # Delete from SFTP if download/upload successful and deletion enabled
            sftp_delete_success = True
            if delete_from_sftp:
                sftp_delete_success = sftp_manager.delete_file(filename)
                if not sftp_delete_success:
                    results['sftp_delete_failed'].append(filename)

            # Mark as success
            results['success'].append(filename)

    finally:
        # Clean up temp directory if used
        if use_s3:
            try:
                import shutil
                shutil.rmtree(download_dir)
            except:
                pass

    return results


def print_summary(results: dict, use_s3: bool = False):
    """Print operation summary"""
    operation_type = "SFTP TO S3 TRANSFER" if use_s3 else "SFTP DOWNLOAD"

    print("\n" + "=" * 60)
    print(f"{operation_type} SUMMARY")
    print("=" * 60)

    if use_s3:
        print(f"Files successfully transferred to S3: {len(results['success'])}")
    else:
        print(f"Files successfully downloaded: {len(results['success'])}")

    print(f"Files not found on SFTP: {len(results['sftp_not_found'])}")
    print(f"SFTP download failures: {len(results['sftp_download_failed'])}")

    if use_s3:
        print(f"S3 upload failures: {len(results['s3_upload_failed'])}")
        print(f"Files already in S3 (skipped): {len(results['s3_already_exists'])}")

    print(f"SFTP deletion failures: {len(results['sftp_delete_failed'])}")

    if results['success']:
        action = "transferred" if use_s3 else "downloaded"
        print(f"\nSuccessfully {action}: {', '.join(results['success'])}")

    if results['sftp_not_found']:
        print(f"\nNot found on SFTP: {', '.join(results['sftp_not_found'])}")

    if results['sftp_download_failed']:
        print(f"\nSFTP download failed: {', '.join(results['sftp_download_failed'])}")

    if use_s3 and results['s3_upload_failed']:
        print(f"\nS3 upload failed: {', '.join(results['s3_upload_failed'])}")

    if results['sftp_delete_failed']:
        print(f"\nSFTP deletion failed: {', '.join(results['sftp_delete_failed'])}")

    if use_s3 and results['s3_already_exists']:
        print(f"\nAlready exists in S3: {', '.join(results['s3_already_exists'])}")


def main():
    parser = argparse.ArgumentParser(description='Download files from SFTP based on CSV list, optionally upload to S3')

    # Required arguments
    parser.add_argument('csv_file', help='Path to CSV file containing filenames')
    parser.add_argument('--sftp-host', required=True, help='SFTP server hostname')
    parser.add_argument('--sftp-user', required=True, help='SFTP username')
    parser.add_argument('--sftp-password', required=True, help='SFTP password')

    # S3 arguments (optional)
    parser.add_argument('--s3-bucket', help='S3 bucket name (enables S3 upload mode)')
    parser.add_argument('--s3-prefix', default='', help='S3 key prefix/folder (default: root)')
    parser.add_argument('--aws-region', help='AWS region for S3 (uses default if not specified)')
    parser.add_argument('--overwrite-s3', action='store_true', help='Overwrite files that already exist in S3')

    # Other optional arguments
    parser.add_argument('--sftp-port', type=int, default=22, help='SFTP port (default: 22)')
    parser.add_argument('--remote-path', default='/', help='Remote SFTP directory path (default: /)')
    parser.add_argument('--local-path', default='./downloads',
                        help='Local download directory when not using S3 (default: ./downloads)')
    parser.add_argument('--filename-column', default='filename',
                        help='CSV column name containing filenames (default: filename)')
    parser.add_argument('--no-delete', action='store_true',
                        help='Do not delete files from SFTP after successful download/transfer')

    args = parser.parse_args()

    # Determine operation mode
    use_s3 = bool(args.s3_bucket)

    if use_s3:
        logger.info("Starting SFTP to S3 transfer process")
    else:
        logger.info("Starting SFTP download process")

    # Read filenames from CSV or get all files from FTP
    filenames = read_csv_filenames(args.csv_file, args.filename_column)
    
    # If CSV file doesn't exist, we'll get all files from the FTP directory later
    if filenames is None:
        logger.info(f"CSV file {args.csv_file} not found. Will download all files from FTP directory.")
        filenames = []  # Will be populated after FTP connection
        get_all_files = True
    elif not filenames:
        logger.error("No filenames found in CSV file")
        return 1
    else:
        get_all_files = False

    # Initialize SFTP manager
    sftp_manager = SFTPManager(args.sftp_host, args.sftp_user, args.sftp_password, args.sftp_port)

    # Initialize S3 manager if needed
    s3_manager = None
    if use_s3:
        try:
            s3_manager = S3Manager(args.s3_bucket, args.aws_region)
        except Exception as e:
            logger.error(f"Failed to initialize S3 manager: {e}")
            return 1

    try:
        # Connect to SFTP
        if not sftp_manager.connect():
            return 1

        # If no CSV file was found, get all files from the specified directory
        if get_all_files:
            if not sftp_manager.change_directory(args.remote_path):
                logger.error(f"Cannot access remote directory: {args.remote_path}")
                return 1
            
            filenames = sftp_manager.list_files()
            if not filenames:
                logger.warning(f"No files found in SFTP directory: {args.remote_path}")
                return 0
            
            logger.info(f"Found {len(filenames)} files to download: {', '.join(filenames)}")

        # Process files
        results = process_files(
            sftp_manager=sftp_manager,
            s3_manager=s3_manager,
            filenames=filenames,
            remote_sftp_path=args.remote_path,
            local_download_path=args.local_path,
            s3_prefix=args.s3_prefix,
            delete_from_sftp=not args.no_delete,
            overwrite_s3=args.overwrite_s3
        )

        # Print summary
        print_summary(results, use_s3)

        # Return appropriate exit code
        if use_s3:
            total_failures = (len(results['sftp_download_failed']) +
                              len(results['s3_upload_failed']))
        else:
            total_failures = len(results['sftp_download_failed'])

        if total_failures > 0:
            logger.warning(f"Process completed with {total_failures} failures")
            return 1

        logger.info("All files processed successfully")
        return 0

    finally:
        sftp_manager.disconnect()


if __name__ == "__main__":
    exit(main())
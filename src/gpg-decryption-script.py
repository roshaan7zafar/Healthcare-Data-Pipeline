import profile
import sys
sys.path.append('../')
sys.path.append('../../')

import os
import boto3
import botocore
import gnupg
import tempfile
import json
import subprocess
# from metrics import metrics


def decrypt_file(encrypted_file_path, output_file_path):
    try:
        subprocess.run(['gpg', '--output', output_file_path, '--decrypt', encrypted_file_path])
        print(f"Decryption successful. Decrypted file saved to {output_file_path}")
    except subprocess.CalledProcessError as e:
        print(f"Decryption failed. Error: {e}")

metrics_dict = {}
def download_decrypt_upload(config):
    # Extract configuration parameters
    source_bucket = config["decryption"]["source_configuration"]["bucket"]
    source_prefix = config["decryption"]["source_configuration"]["prefix"]
    destination_bucket = config["decryption"]["destination_configuration"]["bucket"]
    destination_prefix = config["decryption"]["destination_configuration"]["prefix"]
    client = config["decryption"]["client"]
    year = config["decryption"]["year"]
    month = config["decryption"]["month"]
    # destination_prefix = f'{destination_prefix}/{client}/{year}/{month}'
    # source_prefix = f'{source_prefix}/{client}/{year}/{month}'

    secret_name = config["decryption"]["secret_name"]
    # Set the AWS profile name
    aws_profile = 'paradocs'
    boto3session = boto3.Session(profile_name=aws_profile)
    # boto3session = boto3.Session()
    secrets_manager = boto3session.client('secretsmanager', region_name='us-east-2')
    s3 = boto3session.client('s3', region_name='us-east-2')
    all_files = []

    # Now you can use the s3 and secrets_manager clients with the specified profile


    try:
        # Retrieve the private key and passphrase from AWS Secrets Manager
        secret_response = secrets_manager.get_secret_value(SecretId=secret_name )
        secret_data = json.loads(secret_response['SecretString'])
        private_key = secret_data.get('privatekey')
        passphrase = secret_data.get('passphrase')
        # Initialize GPG
        gpg = gnupg.GPG()
        gpg.import_keys(private_key)
        file_count = 0
        # List objects in the source S3 bucket
        paginator = s3.get_paginator('list_objects')
        operation_parameters = {
            'Bucket': source_bucket,
            'Prefix': source_prefix,
            'MaxKeys': 300
            }
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for obj in page['Contents']:
                file_key = obj.get('Key')
                all_files.append(file_key)
                if not file_key.__contains__('.xml'): continue
                file_count += 1
                print('Decrypted: ', file_count)
                file_name = os.path.basename(file_key)
                # Download the GPG-encrypted file
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmp_file = os.path.join(tmpdir, file_name)
                    s3.download_file(source_bucket, file_key, tmp_file)
                    dest_filename = tmp_file.replace('.gpg', '')
                    decrypt_file(tmp_file, dest_filename)
                    # Upload the decrypted file to the destination S3 bucket
                    destination_key = os.path.join(destination_prefix, file_name)
                    destination_key = destination_key.replace('.gpg','')
                    # s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=open(tmp_file, 'r').read())
                    try:
                        s3.upload_file(dest_filename, destination_bucket, destination_key)
                    except Exception as e:
                        print('Err:', str(e))
                    print(f"Upload successful. File '{dest_filename}' uploaded to '{destination_bucket}'.")
                    metrics_dict['total_gpg_files'] = file_count
                    # metrics.save_metrics(metrics_dict)			
                    print(f"Decrypted and uploaded to: {destination_prefix}")
        print('Total Files Decypted: ', len(all_files))
    except Exception as e:
        print(f"Error: {e}")
        raise

with open('conf.json', 'r') as conf:
    config = json.loads(conf.read())
# Call the function with the provided configuration
download_decrypt_upload(config)

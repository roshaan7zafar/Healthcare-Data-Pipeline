import json
import boto3
import pandas as pd
import sys
import tempfile
import os
import json
from sqlalchemy import create_engine, text
import warnings

warnings.filterwarnings("ignore")
# Download gap tracker
from datetime import datetime
import hashlib


# Your hash function
def generate_sha256_hash(firstName, lastName, birthDay):
    # Convert the date to YYYYMMDD format
    birth_date_formatted = pd.to_datetime(birthDay).strftime("%Y%m%d")

    # Convert names to title case
    first_name_titlecase = firstName.title()
    last_name_titlecase = lastName.title()

    # Concatenate and hash
    sha_signature = hashlib.sha256(
        (first_name_titlecase + last_name_titlecase + birth_date_formatted).encode()
    ).hexdigest()
    return sha_signature


def get_boto3_session():
    try:
        aws_profile = "paradocs"
        session = boto3.Session(profile_name=aws_profile)
    except Exception as e:
        print("Switching to Default AWS Profile...")
        session = boto3.Session()
    return session


def download_gaptracker(tmpdirname, config, s3_client):
    try:
        gaptracker_path = f"{config['client']}/{config['year']}/{config['month']}/{config['source_configuration']['suffix']}/{config['gaptracker_filename']}"
        print("gaptracker_path", gaptracker_path)
        gaptracker_download_path = os.path.join(
            tmpdirname, config["gaptracker_filename"]
        )
        s3_client.download_file(
            config["source_configuration"]["bucket"],
            gaptracker_path,
            gaptracker_download_path,
        )
        return gaptracker_download_path
    except Exception as e:
        print(f"Error downloading {config['gaptracker_filename']} from S3: {str(e)}")
        sys.exit()


# Extract New Open Gaps sheet
def extract_new_open_gaps_sheet(gaptracker_path, tmpdirname, config):
    gatracker_df = pd.read_excel(
        gaptracker_path, sheet_name=config["opengaps_sheetname"], header=1
    )
    # date_columns = ['DeployedDate', 'birthdate', 'analyticrundate']
    # for col in date_columns:
    #     gatracker_df[col] = gatracker_df[col].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if isinstance(x, pd.Timestamp) else x)
    open_gaps_csv_filename = (
        f"OGT_{config['year']}{config['month']}_{config['client']}.csv"
    )
    open_gaps_csv_path = os.path.join(tmpdirname, open_gaps_csv_filename)
    gatracker_df.to_csv(open_gaps_csv_path, index=False)
    return open_gaps_csv_path


# Run Optrim script
# Returns OpenQualityGaps , OpenRiskGaps and ToChartpath paths
def run_optrim_script(open_gaps_csv_path, tmpdirname, config):
    openqualitygaps_filename = (
        f"{config['year']}{config['month']}_{config['client']}_OpenQualityGaps.csv"
    )
    openriskgaps_filename = (
        f"{config['year']}{config['month']}_{config['client']}_OpenRiskGaps.csv"
    )
    tocharpath_filename = (
        f"{config['year']}{config['month']}_{config['client']}_toChartPath.csv"
    )

    openqualitygaps_csv_path = os.path.join(tmpdirname, openqualitygaps_filename)
    openriskgaps_csv_path = os.path.join(tmpdirname, openriskgaps_filename)
    tocharpath_csv_path = os.path.join(tmpdirname, tocharpath_filename)

    # Load the New Open Gaps CSV
    new_open_gaps_df = pd.read_csv(open_gaps_csv_path)

    # ------------------------------------
    # Generate CLIENT_OpenRiskGaps.csv
    # ------------------------------------
    # Condition 1: Keep only rows where 'gaptype' is 'RISK'
    all_risk_gaps_df = new_open_gaps_df[new_open_gaps_df["gaptype"] == "RISK"]
    # Condition 2: Keep only rows where 'gatstatus' is 'OPEN'
    open_risk_gaps_df = all_risk_gaps_df[all_risk_gaps_df["gapstatus"] == "OPEN"]

    open_risk_gaps_df["pdpin"] = open_risk_gaps_df.apply(
        lambda row: generate_sha256_hash(
            row["memberfirstname"], row["memberlastname"], row["birthdate"]
        ),
        axis=1,
    )
    # Save the Open Risk Gaps as CSV
    open_risk_gaps_columns = [
        "pdpin",
        "clientmemberid",
        "uniquememberid",
        "uniquemembergapid",
        "ruleidentifier",
        "gapdescription",
        "gapreason",
        "uniqueprovidergroupid",
        "Submitted Date",
    ]
    open_risk_gaps_df_to_db = open_risk_gaps_df[open_risk_gaps_columns]
    open_risk_gaps_df_to_db.to_csv(openriskgaps_csv_path, index=False)
    # ------------------------------------

    # ------------------------------------
    # Generate CLIENT_OpenQualityGaps.csv
    # ------------------------------------
    # Condition 1: Keep only rows where 'gaptype' is 'RISK'
    all_quality_gaps_df = new_open_gaps_df[new_open_gaps_df["gaptype"] == "Quality"]
    # Condition 2: Keep only rows where 'gatstatus' is 'OPEN'
    open_quality_gaps_df = all_quality_gaps_df[
        all_quality_gaps_df["gapstatus"] == "OPEN"
    ]
    # Save the Open Risk Gaps as CSV
    open_quality_gaps_df.to_csv(openqualitygaps_csv_path, index=False)
    # ------------------------------------

    # ------------------------------------
    # Generate CLIENT__toCharPath.csv
    # ------------------------------------
    to_chart_path_columns = [
        "memberfirstname",
        "membermiddlename",
        "memberlastname",
        "gender",
        "birthdate",
        "MemberZipCode",
        "clientmemberid",
    ]
    # Extract data for each CSV based on the filtered df
    to_chart_path_df = open_risk_gaps_df[to_chart_path_columns]
    # Remove rows with duplicate values for 'clientmemberid' in the 'ToChartPath' DataFrame
    to_chart_path_df = to_chart_path_df.drop_duplicates(
        subset="clientmemberid", keep="first"
    )

    # Save to specified paths
    to_chart_path_df.to_csv(tocharpath_csv_path, index=False)

    return openqualitygaps_csv_path, openriskgaps_csv_path, tocharpath_csv_path


def upload_csv(destination_bucket, destination_prefix, csv_filepath):
    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file(csv_filepath, destination_bucket, destination_prefix)
        print(f"Successfully uploaded {csv_filepath} to S3.")
    except Exception as e:
        print(f"Error uploading {csv_filepath} to S3: {str(e)}")
        sys.exit()


def insert_openriskgaps_to_db(config, openriskgaps_csv_path, rds_secret_string):
    rds_secret = json.loads(rds_secret_string)
    host = rds_secret["host"]
    username = rds_secret["username"]
    password = rds_secret["password"]
    engine = rds_secret["engine"]
    port = rds_secret["port"]
    db_name = rds_secret["dbname"]
    schema = config["rds"]["schema_name"]
    table_name = config["rds"]["risk_gaps_table_name"]

    if engine == "postgres":
        engine = "postgresql"

    engine_creds = f"{engine}://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(
        engine_creds, connect_args={"options": f"-csearch_path={schema}"}
    )

    # Read CSV into DataFrame
    df = pd.read_csv(openriskgaps_csv_path)
    df["month"] = config["month"]
    df["year"] = config["year"]
    df["client"] = config["client"]
    df["Appointment Date"] = ""
    df["Gap Tracker"] = ""
    timestamp = str(datetime.now())
    df["created_at"] = timestamp
    df["updated_at"] = timestamp

    # Specify the raw SQL DELETE statement with the condition
    delete_query = f"DELETE FROM {table_name} WHERE client = '{config['client']}' and month = '{config['month']}' and year = '{config['year']}';"  # Change this query as needed
    delete_query = text(delete_query)
    # Execute the raw SQL query
    try:
        with engine.connect() as connection:
            result = connection.execute(delete_query)
            print("result", result)
            connection.commit()
    except Exception as e:
        print("Exception while flushing table", str(e))
    # Insert DataFrame into PostgreSQL database
    df.to_sql(
        name=table_name, con=engine, schema=schema, index=False, if_exists="append"
    )  # make this append later
    print("Stored in Database")


def get_secret(config, secret_name, region_name):
    # Create a Secrets Manager client
    session = get_boto3_session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        print(f"Error getting SecretString from Secrets Manager: {str(e)}")
        sys.exit()

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    return secret


def main():
    with open("src/eventus_conf.json", "r") as conf:
        config = json.loads(conf.read())
        dynamic_part = f"{config['client']}/{config['year']}/{config['month']}/{config['destination_configuration']['suffix']}"

    if config is None:
        print("ERROR: Problem parsing config file")
        sys.exit()

    with tempfile.TemporaryDirectory() as tmpdirname:
        session = get_boto3_session()
        s3_client = session.client("s3")
        gaptracker_download_path = download_gaptracker(tmpdirname, config, s3_client)
        open_gaps_csv_path = extract_new_open_gaps_sheet(
            gaptracker_download_path, tmpdirname, config
        )
        (
            openqualitygaps_csv_path,
            openriskgaps_csv_path,
            tocharpath_csv_path,
        ) = run_optrim_script(open_gaps_csv_path, tmpdirname, config)
        # upload csvs and encrypted tocharpath
        csvs_paths = [
            open_gaps_csv_path,
            openqualitygaps_csv_path,
            openriskgaps_csv_path,
            tocharpath_csv_path,
        ]
        for csv_path in csvs_paths:
            destination_bucket = config["destination_configuration"]["bucket"]
            csv_filename = os.path.basename(csv_path)
            upload_csv(destination_bucket, f"{dynamic_part}/{csv_filename}", csv_path)

        # GET RDS SECRET
        secret_name = config["secrets_manager"]["rds"]["secret_name"]
        region_name = config["secrets_manager"]["rds"]["region_name"]
        rds_secret = get_secret(config, secret_name, region_name)
        # insert openriskgaps into rds datatbase
        insert_openriskgaps_to_db(config, openriskgaps_csv_path, rds_secret)


main()

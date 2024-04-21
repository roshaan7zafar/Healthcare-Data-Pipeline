# %%
import boto3
import pandas as pd
from io import StringIO
import io
import json
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import sys

warnings.filterwarnings("ignore")

config_file_paths = [
    "src/eventus_conf.json",
    "src/cpa_conf.json",
    # Add more paths as needed
]


def load_config(file_path):
    try:
        with open(file_path, "r") as conf:
            return json.loads(conf.read())
    except Exception as e:
        print(f"ERROR: Problem parsing config file {file_path}")
        print(f"Exception: {str(e)}")
        sys.exit()


# with open("src/cpa_conf.json", "r") as conf:
#     config = json.loads(conf.read())
#     dynamic_part = f"{config['client']}/{config['year']}/{config['month']}/{config['destination_configuration']['suffix']}"

# if config is None:
#     print("ERROR: Problem parsing config file")
#     sys.exit()

# %%


# %%
def select_open_risk_records(config):
    rds_secret = get_secret("dev/rds")
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
    connection = engine.connect()
    # Construct the SQL query based on the provided month and year
    query = text(
        f"SELECT * FROM {table_name} where client = '{config['client']}' AND year = '{config['year']}' AND month = '{config['month']}' "
    )

    # Execute the query and load the results into a DataFrame
    open_risk_gaps = pd.read_sql(query, connection)
    # open_risk_gaps["clientmemberid"] = open_risk_gaps["clientmemberid"].astype(str)
    open_risk_gaps["ruleidentifier"] = open_risk_gaps["ruleidentifier"].astype(str)
    open_risk_gaps.rename(columns={"ruleidentifier": "hcc_code"}, inplace=True)
    open_risk_gaps["pdpin"] = open_risk_gaps["pdpin"].astype(str)
    open_risk_gaps["Appointment Date"] = ""
    open_risk_gaps["Gap Tracker"] = ""
    open_risk_gaps["Notes"] = ""

    # Close the database connection
    connection.close()
    engine.dispose()

    return open_risk_gaps


# %%


def get_secret(secret_name, region_name="us-east-2"):
    # Create a Secrets Manager client
    boto3_session = get_boto3_session()
    client = boto3_session.client(
        service_name="secretsmanager", region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        print(f"Error getting SecretString from Secrets Manager: {str(e)}")
        sys.exit()

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    secret = json.loads(secret)
    return secret


# %%
def get_boto3_session():
    aws_profile = "paradocs"
    try:
        session = boto3.Session(profile_name=aws_profile)
    except Exception as e:
        print("Error loading paradocs profile")
        session = boto3.Session()
    return session


# %%
def read_CodeX_files(s3_client, bucket_name, prefix):
    all_dataframes = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        # Extract keys from the current page
        keys = [obj["Key"] for obj in page.get("Contents", [])]
        # Skip if there are no keys
        if not keys:
            continue

        for key in keys:
            if not key.endswith(".csv"):
                continue
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            csv_content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            all_dataframes.append(df)
            print("Iteration#: ", len(all_dataframes))
            # if len(all_dataframes) > 10:
            #     break

    consolidated_df = pd.concat(all_dataframes, ignore_index=True)
    consolidated_df = consolidated_df.drop("clientMemberId", axis=1)
    # consolidated_df["clientMemberId"] = consolidated_df["clientMemberId"].astype(str)
    consolidated_df.rename(columns={"PD_Pin": "pdpin"}, inplace=True)
    consolidated_df["pdpin"] = consolidated_df["pdpin"].astype(str)
    # consolidated_df.rename(columns={"clientMemberId": "clientmemberid"}, inplace=True)
    consolidated_df = consolidated_df[consolidated_df["codeType"] == "ICD10"]
    consolidated_df["ICD_Code"] = consolidated_df["ICD_Code"].str.replace(".", "")
    consolidated_df = consolidated_df.drop_duplicates()
    consolidated_df = consolidated_df.sort_values("Appointment_Date")
    consolidated_df = consolidated_df.drop_duplicates(
        subset=["pdpin", "ICD_Code", "codeType", "ICD_Name"],
        keep="last",
    )
    return consolidated_df


# %%
def map_icd10_to_hcc(patients_df, icd10_to_hcc_df):
    merged_df = pd.merge(patients_df, icd10_to_hcc_df, how="left", on="ICD_Code")
    merged_df.loc[merged_df["hcc_code"] == "NA", "hcc_code"] = np.NaN
    merged_df["hcc_code"] = merged_df["hcc_code"].astype(str)
    merged_df["hcc_code"] = merged_df["hcc_code"].str.split(".").str[0]

    return merged_df


# %%
def load_hcc_lookup_table(config):
    bucket = config["midyear_final_bucket"]
    midyear_final_filename = config["midyear_final_filename"]
    midyear_final_column_names = config["midyear_final_column_names"]
    midyear_file_key = f"{midyear_final_filename}"
    boto3_session = get_boto3_session()
    s3_client = boto3_session.client("s3")
    obj = s3_client.get_object(Bucket=bucket, Key=midyear_file_key)
    midyear_final_df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    hcc_lookup_df = midyear_final_df[midyear_final_column_names]
    hcc_lookup_df.columns = ["ICD_Code", "hcc_code"]
    hcc_lookup_df = hcc_lookup_df.fillna("NA")
    hcc_lookup_df = hcc_lookup_df.drop_duplicates()
    return hcc_lookup_df


# %%
def update_appointment_dates(patient_dict):
    rds_secret = get_secret("dev/rds")
    timestamp = str(datetime.now())
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
    # Define your SQLAlchemy engine

    try:
        # Iterate through the dictionary and update the database
        with engine.connect() as connection:
            for uniquemembergapid, inner_dict in patient_dict.items():
                # Construct the raw SQL update statement
                update_query = f"""UPDATE {table_name} SET "Appointment Date" = {inner_dict.get('appointment_date')}, pdpin = '{inner_dict.get('pdpin')}', "Gap Tracker" = 'Assessed and Present', updated_at = '{timestamp}' WHERE uniquemembergapid = '{uniquemembergapid}';"""
                update_query = text(update_query)
                # Execute the raw SQL update statement
                connection.execute(update_query)
            connection.commit()

    except Exception as e:
        # Handle exceptions (rollback, log, etc.)
        print(f"Error: {e}")


def insert_into_codex_db(config, df):
    rds_secret = get_secret("dev/rds")
    host = rds_secret["host"]
    username = rds_secret["username"]
    password = rds_secret["password"]
    engine = rds_secret["engine"]
    port = rds_secret["port"]
    db_name = rds_secret["dbname"]
    schema = config["rds"]["schema_name"]
    table_name = config["rds"]["codex_table_name"]

    if engine == "postgres":
        engine = "postgresql"

    engine_creds = f"{engine}://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(
        engine_creds, connect_args={"options": f"-csearch_path={schema}"}
    )

    # Read CSV into DataFrame
    # df = pd.read_csv(dataframe)
    df["gap_tracker"] = ""
    df["month"] = config["month"]
    df["year"] = config["year"]
    df["client"] = config["client"]
    # df["Appointment Date"] = ""
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


##
boto3_session = get_boto3_session()

s3_client = boto3_session.client("s3", region_name="us-east-2")

for file_path in config_file_paths:
    config = load_config(file_path)

    bucket_name = config["post-processing"]["bucket"]

    prefix = f"{config['client']}/{config['year']}/{config['month']}/codex/"

    patients_df = read_CodeX_files(s3_client, bucket_name, prefix)

    hcc_lookup = load_hcc_lookup_table(config)

    patients_hcc_df = map_icd10_to_hcc(patients_df, hcc_lookup)

    patients_hcc_df_rename = patients_hcc_df.rename(
        columns={
            "Appointment_Date": "appointment_date",
            "pdpin": "pd_pin",
            "ICD_Code": "icd_code",
            "codeType": "codetype",
            "ICD_Name": "icd_name",
        }
    )
    #####----insert here -----codex insertion function
    insert_into_codex_db(config, patients_hcc_df_rename)
    # insert
    open_risk_gaps = select_open_risk_records(config)

    matched_df = pd.merge(
        patients_hcc_df, open_risk_gaps, how="inner", on=["pdpin", "hcc_code"]
    )
    matched_df = matched_df.drop_duplicates(subset=["uniquememberid"], keep="last")

    # patients_hcc_df['hcc_code'] = patients_hcc_df['hcc_code'].astype(str)
    # patients_hcc_df['clientmemberid'] = patients_hcc_df['clientmemberid'].astype(str)

    # open_risk_gaps['hcc_code'] = open_risk_gaps['hcc_code'].astype(str)
    # open_risk_gaps['clientmemberid'] = open_risk_gaps['clientmemberid'].astype(str)

    # %%

    matched_dict = (
        matched_df[
            ["uniquemembergapid", "Appointment_Date", "pdpin", "uniqueprovidergroupid"]
        ]
        .set_index("uniquemembergapid")
        .to_dict()
    )
    new_dict = {}
    for k, v in matched_dict["Appointment_Date"].items():
        new_dict[k] = {
            "pdpin": matched_dict["pdpin"][k],
            "appointment_date": v,
            "uniqueprovidergroupid": str(
                matched_dict["uniqueprovidergroupid"][k]
            ).split(".")[0],
        }

    # update_appointment_dates(new_dict)

    file_path = f"matched_patients_dict_{config['client']}.json"
    with open(file_path, "w") as json_file:
        json.dump(new_dict, json_file)

    print(f"Data saved to {file_path}")

    # insert_into_codex_db
    # matched_df.rename(
    #     columns={
    #         "Appointment_Date": "appointment_date",
    #         "pdpin": "pd_pin",
    #         "ICD_Code": "icd_code",
    #         "codeType": "codetype",
    #         "ICD_Name": "icd_name",
    #     },
    #     inplace=True,
    # )
    # matched_df = matched_df[
    #     ["appointment_date", "pd_pin", "icd_code", "codetype", "icd_name"]
    # ]


print(matched_df.columns)
print(matched_df.head(50))

# %%

# %%

# %%
print(matched_dict["Appointment_Date"])

# %%

# %%
print("--------DICT FOR NEXT STEP--------")
print("\n\n\n")
print("\n\n\n")
print(new_dict)
print("\n\n\n")
print("\n\n\n")


# %%
print("done")
print("Total Keys", len(list(new_dict.keys())))
# %% [markdown]
# # ----------------DONE -------------------

# %%
# Save the dictionary to a JSON file

# %%

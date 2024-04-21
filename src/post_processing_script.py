# %%
import boto3
import pandas as pd
from io import StringIO
import io
import json
import warnings
import tempfile
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import sys
import os
from tempfile import TemporaryDirectory
import zipfile
import numpy as np

warnings.filterwarnings("ignore")
import pdf_conversion

from datetime import datetime
import hashlib
import pandas as pd
from datetime import datetime, timedelta

# %%
print("Started")

# %%
# Save the dictionary to a JSON file
# file_path = "matched_patients_dict.json"
file_path = "matched_patients_dict_eventus-whole-health.json"
with open(file_path, "r") as json_file:
    patients = json.load(json_file)

print(f"Data saved to {file_path}")

# %%
print("Loaded")

# %%
with open("src/eventus_conf.json", "r") as conf:
    config = json.loads(conf.read())
    dynamic_part = f"{config['client']}/{config['year']}/{config['month']}/{config['destination_configuration']['suffix']}"

if config is None:
    print("ERROR: Problem parsing config file")
    sys.exit()


def add_not_assessed_to_empty_cells(dataframe):
    time_now = datetime.now()

    # Define a custom function to update 'gap_tracker' within each group
    def update_gap_tracker(group):
        # Check if 'A&P' is present in any row of the group
        has_ap = any(group["Gap Tracker"] == "Assessed and Present")
        sec_filter = group["Gap Tracker"].isna()
        # If 'A&P' is present, update 'gap_tracker' to 'Not Assessed' for non-'A&P' rows
        if has_ap:
            group.loc[
                (group["Gap Tracker"] != "Assessed and Present") & (sec_filter), "Notes"
            ] = f"Updated at {time_now}"
            group.loc[
                (group["Gap Tracker"] != "Assessed and Present") & (sec_filter),
                "Gap Tracker",
            ] = "Not Assessed"
        return group

    # Apply the custom function to each group defined by 'unique_member_id'
    dataframe = dataframe.groupby("uniquememberid").apply(update_gap_tracker)
    # Write the updated DataFrame to a new CSV file
    return dataframe


# %%
def zip_pdfs_in_folder(folder_name, output_zip_name):
    # Check if the folder exists
    if not os.path.exists(folder_name):
        print(f"Folder '{folder_name}' not found.")
        return

    # List all PDF files in the folder
    pdf_files = [
        f for f in os.listdir(folder_name) if f.endswith(".pdf") or f.endswith(".xlsx")
    ]

    # Check if there are any PDF files to zip
    if not pdf_files:
        print(f"No PDF files found in '{folder_name}'.")
        return

    # Create a ZIP file for compression
    with zipfile.ZipFile(output_zip_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for pdf_file in pdf_files:
            pdf_file_path = os.path.join(folder_name, pdf_file)
            zipf.write(pdf_file_path, os.path.basename(pdf_file))

    print(f"PDF files in '{folder_name}' have been zipped to '{output_zip_name}'.")


# %%


def get_boto3_session():
    try:
        aws_profile = "paradocs"
        session = boto3.Session(profile_name=aws_profile)
    except Exception as e:
        print("Switching to Default AWS Profile...")
        session = boto3.Session()
    return session


# %%


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


# %%


def replace_worksheet_with_dataframe(
    dataframe, gaptracker_download_path, new_open_gaps_sheetname
):
    try:
        with pd.ExcelWriter(
            gaptracker_download_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace",
        ) as writer:
            dataframe.to_excel(writer, sheet_name=new_open_gaps_sheetname, index=False)
    #         s3 = boto3session.resource('s3')
    #         dest_key = f'{destination_prefix}/{client}/{year}/{month}/{active_gap_tracker_filename}'
    #         s3.Object(destination_bucket, dest_key).put(Body=open(tmp_file, 'rb'))
    except Exception as e:
        print(f"Could not update Assesment and Gap Tracker values\n\n", str(e))


# %%
def replace_sheet(new_open_gaps_df, patient_dict):
    time_now = datetime.now()
    # Extract the first row and save it for later use
    first_row = new_open_gaps_df.iloc[0]

    # Rename the columns to use the second row as column names
    new_open_gaps_df.columns = new_open_gaps_df.iloc[1]

    # Drop the first and second rows
    new_open_gaps_df = new_open_gaps_df.iloc[2:]
    actual_columns = new_open_gaps_df.columns
    count = 0

    new_open_gaps_rows = len(new_open_gaps_df)
    print("Total Rows in New Open Gaps: ", new_open_gaps_rows)

    fil = new_open_gaps_df["Appointment Date"].str.strip().str.len() == 0
    new_open_gaps_df.loc[fil, "Appointment Date"] = np.NaN
    # new_open_gaps_df['Appointment Date'].fillna('NA')

    len(new_open_gaps_df["Appointment Date"])
    new_open_gaps_df["Appointment Date"].count()
    print(
        "Lets verify",
        len(new_open_gaps_df[~new_open_gaps_df["Appointment Date"].isna()]),
    )
    #     patient_dict = { 'uniquemembergapid':'', 'Appointment_Date':''}

    print("total_matched_patients", len(patient_dict.keys()))

    for uniquemembergapid, inner_dict in patient_dict.items():
        condition = uniquemembergapid == new_open_gaps_df["uniquemembergapid"]
        new_open_gaps_df.loc[condition, "Appointment Date"] = inner_dict.get(
            "appointment_date"
        )
        new_open_gaps_df.loc[condition, "Gap Tracker"] = "Assessed and Present"
        new_open_gaps_df.loc[condition, "Notes"] = f"Updated at {time_now}"
        count += 1

    new_open_gaps_df["DeployedDate"] = pd.to_datetime(
        new_open_gaps_df["DeployedDate"], errors="coerce"
    )
    new_open_gaps_df["birthdate"] = pd.to_datetime(
        new_open_gaps_df["birthdate"], errors="coerce"
    )
    new_open_gaps_df["analyticrundate"] = pd.to_datetime(
        new_open_gaps_df["analyticrundate"], errors="coerce"
    )
    new_open_gaps_df["DeployedDate"] = new_open_gaps_df["DeployedDate"].dt.strftime(
        "%#m/%#d/%y"
    )
    new_open_gaps_df["birthdate"] = new_open_gaps_df["birthdate"].dt.strftime(
        "%#m/%#d/%y"
    )
    new_open_gaps_df["analyticrundate"] = new_open_gaps_df[
        "analyticrundate"
    ].dt.strftime("%#m/%#d/%y")

    new_open_gaps_rows_post_processing = len(new_open_gaps_df)
    print("Total Rows in New Open Gaps: ", new_open_gaps_rows_post_processing)
    print("Total Unique Filtered Patients (# of rows updated): ", count)
    print(
        "Lets verify again",
        len(new_open_gaps_df[~new_open_gaps_df["Appointment Date"].isna()]),
    )

    print("Not Assessed Count", len(new_open_gaps_df["Gap Tracker"]))
    new_open_gaps_df["Gap Tracker"] = new_open_gaps_df["Gap Tracker"].str.strip()
    print("Not Accessed Logic Started")
    print(
        "Not Assessed Count",
        len(new_open_gaps_df[new_open_gaps_df["Gap Tracker"] == "Not Assessed"]),
    )
    new_open_gaps_df = add_not_assessed_to_empty_cells(new_open_gaps_df)
    print("Not Accessed Logic Complete")
    print(
        "Not Assessed Count",
        len(new_open_gaps_df[new_open_gaps_df["Gap Tracker"] == "Not Assessed"]),
    )
    # new_open_gaps_df.to_csv("not_assessed_open_gap_risk.csv", index=False)
    ###code goes here
    metrics_after_transformation(new_open_gaps_df)

    # attach pdpin column to new_open_gaps_df because we will need in next step to extract unique pdpins
    new_open_gaps_df["pdpin"] = new_open_gaps_df.apply(
        lambda row: generate_sha256_hash(
            row["memberfirstname"], row["memberlastname"], row["birthdate"]
        ),
        axis=1,
    )

    # read data from codex table from database
    codex_table_df = select_codex_files_db_records(config)

    # Cna- not present Logic starts below
    new_open_gaps_df = update_gap_tracker_column(new_open_gaps_df, codex_table_df)

    # drop column pdpin
    new_open_gaps_df.drop(new_open_gaps_df.columns["pdpin"], axis=1, inplace=True)

    new_open_gaps_df = pd.concat(
        [actual_columns.to_frame().T, new_open_gaps_df], ignore_index=True
    )
    new_open_gaps_df.columns = first_row
    return new_open_gaps_df


# %%
def get_unique_member_dict(ogt_filepath="ogt.xlsx"):
    new_open_gaps_df = pd.read_excel(ogt_filepath, "New Open Gaps", skiprows=1)
    member_mappings = (
        new_open_gaps_df[["uniquememberid", "uniquemembergapid"]]
        .set_index("uniquemembergapid")
        .to_dict()
    )
    member_mappings = member_mappings.get("uniquememberid")
    return member_mappings


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


def generate_sha256_hash(firstName, lastName, birthDay):
    if pd.isna(firstName) or pd.isna(lastName) or pd.isna(birthDay):
        return None
    # Convert the date to YYYMMDD format
    # date_object = datetime.strptime(date_string, "%m/%d/%y")
    birth_date_formatted = pd.to_datetime(birthDay, format="%m/%d/%y")

    # birth_date_formatted = pd.to_datetime(birthDay).strftime("%m%d%y")

    # Convert names to title case

    first_name_titlecase = firstName.title()
    last_name_titlecase = lastName.title()

    # Concatenate and hash
    sha_signature = hashlib.sha256(
        (first_name_titlecase + last_name_titlecase + birth_date_formatted).encode()
    ).hexdigest()
    return sha_signature


def select_codex_files_db_records(config):
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
    connection = engine.connect()
    # Construct the SQL query based on the provided month and year
    query = text(
        # f"SELECT * FROM {table_name} where client = '{config['client']}' AND year = '{config['year']}' AND month = '{config['month']}' "
        f"SELECT * FROM {table_name}"
    )

    # Execute the query and load the results into a DataFrame
    codex_table_df = pd.read_sql(query, connection)
    # open_risk_gaps["clientmemberid"] = open_risk_gaps["clientmemberid"].astype(str)
    # open_risk_gaps["ruleidentifier"] = open_risk_gaps["ruleidentifier"].astype(str)
    # open_risk_gaps.rename(columns={"ruleidentifier": "hcc_code"}, inplace=True)
    # open_risk_gaps["pdpin"] = open_risk_gaps["pdpin"].astype(str)
    # open_risk_gaps["Appointment Date"] = ""
    # open_risk_gaps["Gap Tracker"] = ""
    # open_risk_gaps["Notes"] = ""

    # Close the database connection
    connection.close()
    engine.dispose()

    return codex_table_df


def update_gap_tracker_column(gaptracker_df, codex_df):
    # Assuming 'Appointment Date' is the name of the date column in your Codex table
    codex_df["appointment_date"] = pd.to_datetime(codex_df["appointment_date"])

    # Select unique PDPins where 'Gap Tracker' column is empty
    empty_gap_tracker_pdpins = gaptracker_df[
        gaptracker_df["Gap Tracker"].str.strip().eq("")
    ]["pdpin"].unique()

    # Loop through unique PDPins with empty 'Gap Tracker' column
    for pdpin in empty_gap_tracker_pdpins:
        # Check if there is any record of PDPin in the codex table
        if pdpin in codex_df["pd_pin"].values:
            # Check if the record is within the last 3 months
            three_months_ago = datetime.now() - timedelta(days=90)
            recent_record = codex_df[
                (codex_df["pd_pin"] == pdpin)
                & (codex_df["appointment_date"] >= three_months_ago)
            ]

            if not recent_record.empty:
                # if present in recent 3 months, leave it as is
                # Update the 'Gap Tracker' column with 'Latest'
                #                 gaptracker_df.loc[(gaptracker_df['PDPin'] == pdpin) & (gaptracker_df['Gap Tracker'].str.strip().eq('')), 'Gap Tracker'] = 'Present'

                pass
            else:
                # Update the 'Gap Tracker' column with 'CNA - Not Present' if not present recently but present in codex database
                gaptracker_df.loc[
                    (gaptracker_df["pdpin"] == pdpin)
                    & (gaptracker_df["Gap Tracker"].str.strip().eq("")),
                    "Gap Tracker",
                ] = "CNA - Not Present"
        else:
            # Update the 'Gap Tracker' column with 'CNA - Not Present' if it isn't present in codex database
            gaptracker_df.loc[
                (gaptracker_df["pdpin"] == pdpin)
                & (gaptracker_df["Gap Tracker"].str.strip().eq("")),
                "Gap Tracker",
            ] = "CNA - Not Present"

    return gaptracker_df


# %%
with tempfile.TemporaryDirectory() as tmpdirname:
    session = get_boto3_session()
    s3_client = session.client("s3")
    gaptracker_download_path = download_gaptracker(tmpdirname, config, s3_client)
    new_open_gaps_df = pd.read_excel(
        gaptracker_download_path, "New Open Gaps", header=None
    )

    ### Todo -- original metrics function before transformation
    def metrics_before_transformation(new_open_gaps_df):
        first_row = new_open_gaps_df.iloc[0]

        # Rename the columns to use the second row as column names
        new_open_gaps_df.columns = new_open_gaps_df.iloc[1]

        # Drop the first and second rows
        new_open_gaps_df = new_open_gaps_df.iloc[2:]
        unique_row_count = new_open_gaps_df["uniquemembergapid"].nunique()
        total_gap_types_count = new_open_gaps_df["gaptype"].nunique()
        risk_df = new_open_gaps_df[new_open_gaps_df["gaptype"] == "RISK"]  # 1323
        risk_hcc_count = (
            risk_df.groupby(["gaptype", "category"])["hcccode"]
            .nunique()
            .reset_index(name="risk_category_count")
        )
        #     risk_df = new_open_gaps_df[new_open_gaps_df['gaptype'] == 'RISK']
        #     risk_hcc_count = risk_df.groupby('gaptype')['hcccode'].nunique()
        quality_df = new_open_gaps_df[new_open_gaps_df["gaptype"] == "Quality"]  # 345
        quality_hcc_count = (
            quality_df.groupby(["gaptype", "category"])["hcccode"]
            .nunique()
            .reset_index(name="quality_category_count")
        )

    #     quality_df = new_open_gaps_df[new_open_gaps_df['gaptype'] == 'Quality']
    #     quality_hcc_count = quality_df.groupby('gaptype')['hcccode'].nunique()

    ### Todo --- after transformation
    def metrics_after_transformation(assessed_df):
        total_gaps = len(
            assessed_df
        )  # we get this number by reading assessed df = 1669
        closing_gaps_count = len(
            assessed_df[
                assessed_df["Gap Tracker"].isin(
                    ["Assessed and Present", "Not Assessed"]
                )
            ]
        )
        print(
            len(
                assessed_df[
                    assessed_df["Gap Tracker"].isin(
                        ["Assessed and Present", "Not Assessed"]
                    )
                ]
            )
        )  # 648 closing gaps
        assessed_risk_df = assessed_df[assessed_df["gaptype"] == "RISK"]  # 1323
        assessed_quality_df = assessed_df[assessed_df["gaptype"] == "Quality"]  # 345
        remaining_risk_gaps = (total_gaps - closing_gaps_count) / len(assessed_risk_df)
        remaining_quality_gaps = (total_gaps - closing_gaps_count) / len(
            assessed_quality_df
        )
        assessed_cna_count = assessed_df[
            assessed_df["Gap Tracker"] == "Cna - Not Present"
        ].nunique()

    metrics_before_transformation(new_open_gaps_df)

    df = replace_sheet(new_open_gaps_df.copy(), patients)
    # replace_worksheet_with_dataframe(df, gaptracker_download_path, "New Open Gaps")
    # memberid_dict = get_unique_member_dict(gaptracker_download_path)
    # xmls_dict = {}
    # for uniquemembergapid, inner_dict in patients.items():
    #     s3_xml_key = (
    #         inner_dict.get("pdpin")
    #         + "_"
    #         + str(inner_dict.get("appointment_date"))
    #         + ".xml"
    #     )
    #     uniqueprovidergroupid = inner_dict.get("uniqueprovidergroupid")
    #     new_filename = f"{memberid_dict.get(uniquemembergapid)}_{str(inner_dict.get('appointment_date'))}"
    #     xmls_dict.update(
    #         {
    #             s3_xml_key: {
    #                 "unique_providergroup_id": uniqueprovidergroupid,
    #                 "new_filename": new_filename,
    #             }
    #         }
    #     )

    # for xml_filename, inner_dict in xmls_dict.items():
    #     print("FileName", xml_filename)
    #     print("newfilename", inner_dict.get("new_filename"))
    #     print("unique_providergroup_id", inner_dict.get("unique_providergroup_id"))

    #     if "eventus" in config["client"]:
    #         source_bucket = config["source_configuration"]["bucket"]
    #         xmls_key = f"{config['client']}/{config['year']}/{config['month']}/xmls/{xml_filename}"
    #         local_path = os.path.join(tmpdirname, os.path.basename(xml_filename))
    #         print(local_path)
    #         s3_client.download_file(source_bucket, xmls_key, local_path)
    #         pdf_conversion.main(
    #             local_path,
    #             tmpdirname,
    #             inner_dict.get("new_filename"),
    #             inner_dict.get("unique_providergroup_id"),
    #         )
    #     else:
    #         # print('Qualified File', xml_filename, 'Renamed to ', inner_dict.get("new_filename"))
    #         source_bucket = "mirth-ccd-zone"
    #         html_filename = xml_filename.split("_")[0]
    #         html_filename = html_filename + ".html"
    #         html_key = f"quarantine/{config['client']}/{config['year']}/{config['month']}/{html_filename}"
    #         new_filename = inner_dict.get("new_filename") + ".html"
    #         local_path = os.path.join(tmpdirname, os.path.basename(html_filename))
    #         print("---------------------------------")
    #         print(source_bucket, html_key, local_path)
    #         print("---------------------------------")
    #         print("---------------------------------")
    #         print("----------- DOWNLOADING -----------")
    #         print("---------------------------------")
    #         s3_client.download_file(source_bucket, html_key, local_path)
    #         print("----------- PDF CONVERSION -----------")
    #         print("HTML", local_path)
    #         print("PDF", new_filename.replace("html", "pdf"))

    #         pdf_conversion.html_to_pdf(
    #             local_path, tmpdirname + "/" + new_filename.replace("html", "pdf")
    #         )
    #         print("---------------------------------")

    # print("Started zipping...")
    # output_zip_name = f"optum-submission-for-{config['client']}-{config['year']}-{config['month']}-v2.zip"  # Replace with your desired output ZIP file name
    # zip_pdfs_in_folder(tmpdirname, output_zip_name)
    # print("Zipping Done!")
    # print("Uplaoding!")
    # s3 = get_boto3_session().resource("s3")
    # s3.Object("gold-ccd-zone", f"{output_zip_name}").put(
    #     Body=open(output_zip_name, "rb")
    # )
    # print("Success!!!")

# %%


# %%


# %%


# %%


# %%


# %%

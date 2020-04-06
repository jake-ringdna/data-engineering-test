#!/usr/bin/env python3

# ####################################################################
#
# cleanup_tsv
#
# Cleanse invalid rows from sample TSV from RingDNA and load cleansed
# file into 'users' table in Redshift
#
# Usage:
#
# - These variables must be set in the host environment:
#     REDSHIFT_USERNAME
#     REDSHIFT_PASSWORD
#
# - See "variables that need changed" section below
#
# ####################################################################

import boto3
import csv
import os
import psycopg2


# validate row
def is_valid_row(test_row):
    # check # of columns
    if len(test_row) != 5:
        return False
    # sanity check last column (email) contains '@'
    if '@' not in test_row[4]:
        return False
    # ensure first column (id) is an int
    try:
        int(test_row[0])
        return True
    except ValueError:
        return False

# end is_valid_row()


# attempt to cleanse an invalid row
def fix_bad_row(bad_row):
    # strip spaces and remove blank columns
    candidate_row = [bad_row[0]] + \
        [x.strip() for x in bad_row[1:] if x.strip() != '']
    if is_valid_row(candidate_row):
        return candidate_row

    # check for account_number not starting with a digit
    # if not, assume 3 name fields
    if not candidate_row[3][0].isdigit():

        # 2nd and 3rd names the same: delete duplicate
        if candidate_row[2] == candidate_row[3]:
            del candidate_row[3]

        # otherwise join 2nd and 3rd names together
        else:
            candidate_row[2] = " ".join(candidate_row[2:4])
            del candidate_row[3]

        # check for validity now
        if is_valid_row(candidate_row):
            return candidate_row
        # unaccounted for case
        else:
            return ['-1'] + bad_row

    # unaccounted for case
    else:
        return ['-1'] + bad_row

# end fix_bad_row()


# upload file to an S3 bucket
def upload_to_s3(local_file, bucket, s3_file, profile):

    try:
        session = boto3.Session(profile_name=profile)
        s3_client = session.client('s3')
        s3_client.upload_file(local_file, bucket, s3_file)
        print("   S3 upload is successful")
        return True
    except FileNotFoundError:
        print("   local file not found")
        return False
    except Exception as err:
        print(f"   exception while uploading to S3: {err}")

# end upload_to_s3()


# load from S3 into users table in Redshift
def load_to_redshift(
    bucket, s3_file, host_name, db_name, iam_role_arn, aws_region
):

    '''
    DDL for user table:

    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        first_name VARCHAR(20),
        last_name VARCHAR(30),
        account_number VARCHAR(8) NOT NULL,
        email VARCHAR(80) NOT NULL
    )
    '''

    # get username and password from env variables
    redshift_username = os.environ.get('REDSHIFT_USERNAME')
    redshift_password = os.environ.get('REDSHIFT_PASSWORD')

    try:
        conn = psycopg2.connect(
            host=host_name,
            port=5439,
            user=redshift_username,
            password=redshift_password,
            dbname=db_name)
        print("   successfully connected to Redshift")
    except Exception as err:
        print(f"   error connecting to Redshift: {err}")
        return False

    # Redshift does not enforce primary keys,
    # so truncate table before load, in case of reload
    sql = f'''TRUNCATE users;
              COPY users FROM 's3://{bucket}/{s3_file}'
              iam_role '{iam_role_arn}'
              region '{aws_region}'
              format as csv
              delimiter '\t'
              ignoreheader 1
              emptyasnull;'''

    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close()
        print(f"   data successfully loaded into {db_name}.users table")
    except Exception as err:
        print(f"   error loading into users table from S3: {err}")

# end load_to_redshift()


#
# variables that need changed
#
# would be better to make them command line parameters
# and/or read them from a config file...

# change these if different local filenames are desired
input_filename = "data/data.tsv"
output_filename = "data/cleansed.tsv"

# change these variables for appropriate values for AWS account
s3_bucket = 'mropp-ringdna'
s3_file = 'cleansed.tsv'
config_profile = 'dev'
redshift_host = 'cluster-1.cq4c9ckj1pmi.us-west-1.redshift.amazonaws.com'
db_name = 'dev'
redshift_iam_role = 'arn:aws:iam::891705061311:role/myRedshiftRole'
region = 'us-west-1'

#
# start main script
#
print(f"cleansing TSV: {input_filename}")
with open(input_filename, encoding='utf-16-le', newline='') as inputf, \
        open(output_filename, 'w', encoding='utf-8', newline='') as outputf:
    reader = csv.reader(inputf, delimiter='\t', strict=True)
    writer = csv.writer(outputf, delimiter='\t', quotechar='"',
                        lineterminator='\n')

    # get and write out the header row
    header_row = next(reader)
    writer.writerow(header_row)

    prev_row = []
    for row in reader:
        if is_valid_row(row):
            # write out id as is, with spaces stripped from other columns
            writer.writerow([row[0]] + [x.strip() for x in row[1:]])

        # process invalid row
        else:
            # if prev_row isn't empty, combine with current row
            if prev_row != []:
                combined = prev_row + row
                if is_valid_row(combined):
                    writer.writerow([row[0]] + [x.strip() for x in row[1:]])
                    prev_row = []
                else:
                    # if > 5 items and last item in list contains '@'
                    # then this is a "complete" bad row to be cleansed
                    if len(combined) > 5 and '@' in combined[-1:][0]:
                        fixed_row = fix_bad_row(combined)
                        if fixed_row[0] == '-1':
                            print(f"discarding row that couldn't be cleansed: "
                                  f"{combined}")
                        else:
                            writer.writerow(fixed_row)
                        prev_row = []

                    # otherwise continue to next row with current combined
                    # as previous row value
                    else:
                        prev_row = combined

            # otherwise, start of a bad row
            else:
                prev_row = row

    print(f"output written to: {output_filename}")

print("uploading to S3")
if upload_to_s3(output_filename, s3_bucket, s3_file, config_profile):
    print("loading into Redshift")
    load_to_redshift(
        s3_bucket, s3_file, redshift_host, db_name,
        redshift_iam_role, region
    )

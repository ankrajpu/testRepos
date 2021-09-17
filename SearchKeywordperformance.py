import boto3
# import csv
import pandas as pd
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import io

print("starting the process")

# file = '../sourceData/data.tsv'
output_file = 'processed/' + datetime.today().strftime('%Y-%m-%d') + '_SearchKeywordPerformance.tsv'
bucket_name = 'testbucket'
input_file_name = 'data/data.tsv'


def write_to_s3(client, df, bucket, output_key):
    """
    
    :param client: s3 client for connection 
    :param df: dataframe to be published to s3 
    :param bucket: destination bucket 
    :param output_key: file name and key 
    :return: nothing
    """
    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False, sep ='\t')
        response = client.put_object(
            Bucket=bucket, Key=output_key, Body=csv_buffer.getvalue()
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print("File published to s3:  Status - {status}")
        else:
            print("File publish to s3 failed Status - {status}")


def archive_file():
    print("I am working on it")


def connection_to_s3():
    """
    :return: s3 client for connection 
    :input: nothing for now, but if it a cross account need AWS access ket and secret key
    """
    s3_client = boto3.client(
        "s3"
        # , aws_access_key_id=AWS_ACCESS_KEY_ID
        # , aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        # , aws_session_token=AWS_SESSION_TOKEN
    )
    return s3_client


def read_from_s3(s3_client, source_bucket, key_with_file_name):
    """
    
    :param s3_client: s3 client for connection 
    :param source_bucket: source bucket with data
    :param key_with_file_name: file to be read
    :return:  dataframe
    """

    response = s3_client.get_object(Bucket=source_bucket, Key=key_with_file_name)
    print("i am printing response")
    print(response)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        # read data into dataframe
        df = pd.read_csv(response.get("Body"), sep='\t')
        return df
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")


class ProcessData:

    def __init__(self, b, f):
        """
        :return: local variables
        :input: Bucket name and key
        """
        self.data = []
        source_bucket = b  # get bucket name
        key_with_file_name = f  # get file name

    def transform_data(df):
        """
        :param df: dataframe to be transformed 
        :return: 
        """

        # df = pd.read_csv(df, sep='\t')
        # List to store the extracted data
        domain = []
        keywords = []
        is_external_flag = []
        # Loop all rows in dataframe
        for index, row in df.iterrows():
            k = []
            # parse the URL
            parsed = urlparse(row['referrer'])
            # gives URL - like www.google.com
            referral_domain = parsed.netloc
            # gives the query parameters in the URL
            params = parse_qs(parsed.query)
            # for Internal search, we need to check the query parameters from page_url column
            internal_search_parsed = urlparse(row['page_url'])
            # gives the query parameters in the URL
            internal_search_params = parse_qs(internal_search_parsed.query)
            is_external = 'Yes'
            if referral_domain in ['www.google.com', 'www.bing.com']:
                k = params['q'][0]
            elif referral_domain in ['search.yahoo.com']:
                k = params['p'][0]
            else:
                is_external = 'No'
                if 'k' in params.keys():
                    k = params['k'][0]
                elif 'k' in internal_search_params.keys():
                    k = internal_search_params['k'][0]
                else:
                    k = 'No Keyword'
            # append the data to the default list
            keywords.append(k)
            domain.append(referral_domain)
            is_external_flag.append(is_external)
        # Extend the dataframe and add calculated columns
        df['external_domain'] = domain
        df['is_external'] = is_external_flag
        df['keywords'] = keywords
        # split the product list column in five other columns
        df[['category'
            , 'product_Name'
            , 'number_of_items'
            , 'total_revenue'
            , 'custom_event'
            ]] = df.product_list.str.split(';', expand=True)
        # updates null with 0
        df['total_revenue'] = df['total_revenue'].replace(r'^\s*$', 0, regex=True)
        # updates nan with 0
        df['total_revenue'] = df.total_revenue.fillna(0)
        return df


    def generate_report(df):
        """
        :param df: dataframe
        :return: query result to be written to S3
        """
    
        df['keywords'] = df['keywords'].str.lower()
        df_to_report = df.query('is_external=="Yes"')\
            .groupby(['external_domain', 'keywords'])['total_revenue']\
            .sum()\
            .reset_index()\
            .sort_values(by='total_revenue', ascending=False)
        return df_to_report


def main():
    print("In main and Starting Execution")
    s3_client = connection_to_s3()
    print("connection successful")
    df = read_from_s3(s3_client, bucket_name, input_file_name)
    # x = ProcessData
    processed_data = ProcessData.transform_data(df)
    report = ProcessData.generate_report(processed_data)
    write_to_s3(s3_client,report, bucket_name, output_file)
    


if __name__ == "__main__": 
    main()

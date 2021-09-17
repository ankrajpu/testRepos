import boto3
import pandas as pd
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import io

print("starting the process")

#  file = '../sourceData/data.tsv'
output_file = 'processed/' + datetime.today().strftime('%Y-%m-%d') + '_SearchKeywordPerformance.tsv'
bucket_name = 'testbucket'
file_name = 'data/data.tsv'


def write_to_s3(client, df, bucket, output_key):
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
    print("I am woring on it")


def connection_to_s3():
    s3_client = boto3.client(
        "s3"
        # , aws_access_key_id=AWS_ACCESS_KEY_ID
        # , aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        # , aws_session_token=AWS_SESSION_TOKEN
    )
    return s3_client


def read_from_s3(s3_client, source_bucket, key_with_file_name):
    """
    :return: Object for s3 file
    :input: s3_client, Bucket name and key
    """
    response = s3_client.get_object(Bucket=source_bucket, Key=key_with_file_name)
    print("i am printing response")
    print(response)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
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

        Responsible to process the file
        :return nothing
        :write processed file for S3 bucket
        :Input - File location from S3

        """
        # df = pd.read_csv(file, sep='\t')
        domain = []
        keywords = []
        is_external_flag = []
        for index, row in df.iterrows():
            k = []
            external_domain = urlparse(row['referrer']).netloc
            parsed = urlparse(row['referrer'])
            params = parse_qs(parsed.query)
            internal_search_parsed = urlparse(row['page_url'])
            # internal_domain = urlparse(row['page_url']).netloc
            internal_search_params = parse_qs(internal_search_parsed.query)
            # print(external_domain)
            is_external = 'Yes'
            if external_domain in ['www.google.com', 'www.bing.com']:
                k = params['q'][0]
            elif external_domain in ['search.yahoo.com']:
                k = params['p'][0]
            else:
                is_external = 'No'
                if 'k' in params.keys():
                    k = params['k'][0]
                elif 'k' in internal_search_params.keys():
                    k = internal_search_params['k'][0]
                else:
                    k = 'No Keyword'
            keywords.append(k)
            domain.append(external_domain)
            is_external_flag.append(is_external)
        # the below code works
        df['external_domain'] = domain
        df['is_external'] = is_external_flag
        df['keywords'] = keywords
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
        # print(df)
        # df.to_csv(output, sep='\t')
        return df

    def generate_report(df):
        df['keywords'] = df['keywords'].str.lower()
        final_op = df.query('is_external=="Yes"')\
        .groupby(['external_domain', 'keywords'])['total_revenue']\
        .sum()\
        .reset_index()\
        .sort_values(by='total_revenue', ascending=False)
        # print(final_op)
        return final_op
        


def main():
    print("In main and Starting Execution")
    s3_client = connection_to_s3()
    print("connection successful")
    df = read_from_s3(s3_client, bucket_name, file_name)
    # x = ProcessData
    processed_data = ProcessData.transform_data(df)
    report = ProcessData.generate_report(processed_data)
    write_to_s3(s3_client,report, bucket_name, output_file)
    


if __name__ == "__main__": 
    main()

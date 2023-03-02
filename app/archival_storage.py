import pyarrow.parquet as pq
import pyarrow.json as pj
import os
import boto3
import app.logger as log

logging = log.setup_logger()


class ArchivalClient:

    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.s3_bucket_name = "partner-demo"
        self.json_path = 'app/temp_data_store/json_data'
        self.parquet_path = 'app/temp_data_store/parquet_data'

    def convert_to_parquet(self):
        """
        Convert JSON files to Parquet format.

        Reads all JSON files in the specified directory, converts them to
        Parquet format, and writes the resulting files to a specified
        directory.

        Returns:
            None
        """
        try:
            json_files = os.listdir(self.json_path)

            for file in json_files:
                file_path = os.path.join(self.json_path, file)
                table = pj.read_json(file_path)
                parquet_file_name, _ = os.path.splitext(file)
                parquet_file_name = f"{parquet_file_name}.parquet"
                pq.write_table(table, os.path.join(
                    self.parquet_path, parquet_file_name))
                os.remove(file_path)
        except:
            pass

    def upload_to_S3(self):
        """
        Upload Parquet files from a specified directory to an S3 bucket.

        Uses the Boto3 library to establish a connection to an S3 bucket,
        then uploads all Parquet files found in the specified directory.
        """
        try:
            s3_resource = boto3.resource(
                "s3",
                region_name="us-east-1",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token
            )
            my_bucket = s3_resource.Bucket(self.s3_bucket_name)

            for path, _, files in os.walk(self.parquet_path):
                path = path.replace("\\", "/")
                for file in files:
                    file_path = os.path.join(path, file)
                    my_bucket.upload_file(file_path, 'archive/'+file)
                    os.remove(file_path)

        except Exception as err:
            logging.exception(err)

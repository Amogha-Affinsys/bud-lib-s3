import os
import boto3
from botocore.exceptions import ClientError
import logging
from django.conf import settings

logger = logging.getLogger("s3lib")
EMPTY_STRING = ""


class BotoMinio:
    def __init__(self):
        self.storage = settings.STORAGE_SERVICE
        self.access_key = settings.S3_ACCESS_KEY
        self.secret_key = settings.S3_SECRET_KEY
        self.hostname = settings.S3_INTERNAL_HOST_URL
        self.version = settings.STORAGE_VERSION
        self.external_host = settings.S3_EXTERNAL_HOST_URL if settings.USE_S3_EXTERNAL_CLIENT else settings.S3_INTERNAL_HOST_URL

        self.resource = boto3.resource(self.storage,
                                       endpoint_url=self.hostname,
                                       config=boto3.session.Config(
                                           signature_version=self.version),
                                       aws_access_key_id=self.access_key,
                                       aws_secret_access_key=self.secret_key)

        self.client = boto3.client(self.storage,
                                   endpoint_url=self.hostname,
                                   config=boto3.session.Config(
                                       signature_version=self.version),
                                   aws_access_key_id=self.access_key,
                                   aws_secret_access_key=self.secret_key)

        self.external_client = boto3.client(self.storage,
                                            endpoint_url=self.external_host,
                                            config=boto3.session.Config(
                                                signature_version=self.version),
                                            aws_access_key_id=self.access_key,
                                            aws_secret_access_key=self.secret_key)

    def check_bucket_exist(self, bucket_name):
        """
        This function is used to check whether specified bucket exists or not.
        @param bucket_name: S3 Bucket Name
        @return: Success - Returns True
                 Failure - Returns False
        """
        try:
            response = self.client.head_bucket(
                Bucket=bucket_name,
            )
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            logger.exception(f"Client Error Occured During Checking Bucket Exists (check_bucket_exist) : {e}")
            return False
        except Exception as e:
            logger.exception(f"Unhandled Exception Occured During Checking Bucket Exists (check_bucket_exist) : {e}")
            return False


    def list_files_by_extension(self, bucket_name, file_extension, prefix=None, include_all_prefix=False):
        """
        Lists all the objects from S3 Bucket by given file_extension.
        @param include_all_prefix: True - Checks inside the all the folders(prefix) present in that particular bucket
        @param file_extension: File Extensions to be searched
        @param prefix: Folder prefix
        @param bucket_name: Bucket Name
        @return: True if success else False
        """
        if self.check_bucket_exist(bucket_name) and prefix is not None and include_all_prefix is False:
            logger.debug(f"Searching files inside the Bucket : {bucket_name} with prefix : {prefix}")
            bucket = self.resource.Bucket(bucket_name)
            objects = bucket.objects.filter(Prefix=prefix)
            search_result = [{'Key': o.key} for o in objects if o.key.endswith(file_extension)]
            logger.debug(f"Search Result : {search_result}")
            if search_result:
                return search_result
            else:
                logger.info(
                    f"No files found in path : {prefix} with extension : {file_extension} from Bucket : {bucket_name}")
                return []
        elif self.check_bucket_exist(bucket_name) and prefix is None and include_all_prefix is False:
            logger.debug(
                f"Searching files inside the Bucket : {bucket_name}, Excluding all prefix inside bucket while searching")
            bucket = self.resource.Bucket(bucket_name)
            objects = bucket.objects.all()
            search_result = [{'Key': o.key} for o in objects if o.key.endswith(file_extension) and '/' not in o.key]
            logger.debug(f"Search Result : {search_result}")
            if search_result:
                return search_result
            else:
                logger.info(
                    f"No files found in Bucket : {bucket_name}, with extension : {file_extension}, (Excluded Prefixes)")
                return []
        elif self.check_bucket_exist(bucket_name) and prefix is None and include_all_prefix is True:
            logger.debug(
                f"Searching files inside the Bucket : {bucket_name}, Including all prefix inside bucket while searching")
            bucket = self.resource.Bucket(bucket_name)
            objects = bucket.objects.all()
            search_result = [{'Key': o.key} for o in objects if o.key.endswith(file_extension)]
            logger.debug(f"Search Result : {search_result}")
            if search_result:
                return search_result
            else:
                logger.info(
                    f"No files found in Bucket : {bucket_name}, with extension : {file_extension}, (Including all Prefixes)")
                return []
        else:
            logger.info(f"Bucket Does not Exists or Invalid Parameters Passed")
            return []
    def check_local_file_exist(self, file_name: str) -> bool:
        """
        Helper method to verify if the specified local file exists in the local directory
        @param file_name: File Name or Path to the File
        @return: Success - Returns True
                 Failure - Returns False
        """
        if os.path.exists(file_name):
            return True
        return False

    def check_object_exist(self, bucket_name: str, object_path: str) -> bool:
        """
        Helper method to verify if the specified  file exists in the specified bucket or not.
        @param bucket_name: S3 Bucket Name
        @param object_path: This is the path in S3 (inside Specified Bucket) where file is stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return: Success - Returns True
                 Failure - Returns False
        """
        try:
            response = self.client.head_object(
                Bucket=bucket_name,
                Key=object_path
            )
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            logger.exception(f"Client Error Occured During Checking Bucket Exists (check_object_exist) : {e}")
            return False
        except Exception as e:
            logger.exception(f"Unhandled Exception Occured During Checking Bucket Exists (check_object_exist) : {e}")
            return False
    def generate_pre_signed_link(self, bucket_name: str, object_path: str, expires_in: int) -> str:
        """
        Generates the pre-signed link for the specified object in the S3 Bucket
        @param bucket_name: S3 Bucket Name
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @param expires_in: Expiry Time in seconds
        @return: Success - Returns Pre-Signed link of the object
                 Failure - Returns Empty String
        """
        try:
            if self.check_bucket_exist(bucket_name) and self.check_object_exist(bucket_name, object_path):
                response = self.external_client.generate_presigned_url('get_object',
                                                                       Params={
                                                                           'Bucket': bucket_name,
                                                                           'Key': object_path
                                                                       },
                                                                       ExpiresIn=expires_in)
                return response
            else:
                logger.debug("Failed to Generate Pre-signed link for the specified File")
                return EMPTY_STRING
        except ClientError as e:
            logger.exception(f"Client Error Occured During Generating Pre-signed Link : {e}")
            return EMPTY_STRING
        except Exception as e:
            logger.exception(f"Unhandled Exception Occured During Generating Pre-signed Link : {e}")
            return EMPTY_STRING

    def put_object(self, bucket_name: str, data: bytes, object_path: str, content_type='octet/stream') -> bool:
        """
        This function Adds an object to a bucket.
        You must have WRITE permissions on a bucket to add an object to it.
        @param content_type: MIME Type of the file
        @param bucket_name: S3 Bucket name
        @param data: bytes or file
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return: Success - True
                 Failure - False
        """
        try:
            obj = self.resource.Object(bucket_name, object_path)
            response = obj.put(Body=data, ContentType=content_type)
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as error:
            logger.exception(f"Client Error Occured During PUT Object (put_object): {error}")
            if error.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                return False
            else:
                return False
        except Exception as e:
            logger.exception(f"Unhandled Exception Occured During PUT Object (put_object): {e}")
            return False

    def put_object_and_get_link(self,
                                bucket_name: str,
                                data: bytes,
                                object_path: str,
                                content_type='octet/stream') -> str:
        """
        This function Adds an object to a bucket and returns link to access the file.( This is not a pre-signed link)
        You must have WRITE permissions on a bucket to add an object to it.
        @param content_type: MIME Type of the file
        @param bucket_name: S3 Bucket name
        @param data: bytes or file
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return:  Success - Returns Non Pre-Signed Link of the object
                  Failure - Returns Empty String
        """
        if self.check_bucket_exist(bucket_name):
            if self.put_object(bucket_name=bucket_name, data=data, object_path=object_path, content_type=content_type):
                return self.hostname + bucket_name + '/' + object_path
            else:
                logger.debug(f"Specified Bucket Doesn't Exists Hence Failure in put object (put_object_and_get_link)")
                return EMPTY_STRING
        else:
            logger.exception(f"Specified Bucket Doesn't Exists Hence Failure in put object (put_object_and_get_link)")
            return EMPTY_STRING

    def put_object_and_get_pre_signed_link(self,
                                           bucket_name: str,
                                           data: bytes,
                                           object_path: str,
                                           expires_in=settings.DEFAULT_S3_LINK_EXPIRY_TIMEOUT,
                                           content_type='octet/stream') -> str:
        """
        This function Adds an object to a bucket and returns pre-signed link.
        You must have WRITE permissions on a bucket to add an object to it.
        @param content_type: MIME Type of the file
        @param expires_in: Expiry Time in seconds
        @param bucket_name: S3 Bucket name
        @param data: bytes or file
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return:  Success - Returns Pre-signed Link og the Object
                  Failure - Returns Empty String
        """
        if self.check_bucket_exist(bucket_name):
            if self.put_object(bucket_name=bucket_name, data=data, object_path=object_path, content_type=content_type):
                return self.generate_pre_signed_link(bucket_name=bucket_name,
                                                     object_path=object_path,
                                                     expires_in=expires_in)
            else:
                logger.exception(f"Failed to PUT object into specified Bucket (put_object_and_get_pre_signed_link)")
                return EMPTY_STRING
        else:
            logger.exception(
                f"Specified Bucket Doesn't Exists Hence Failure in put object (put_object_and_get_pre_signed_link)")
            return EMPTY_STRING

    def upload_file(self,
                    bucket_name: str,
                    file_name,
                    object_path: str,
                    content_type='octet/stream') -> bool:
        """
        This function is used to upload file to specified bucket .
        You must have WRITE permissions on a bucket to add an object to it.
        @param content_type: MIME Type of the file.(By default it's considered as octet/stream)
        @param bucket_name: S3 Bucket Name
        @param file_name: File to be uploaded to S3 Bucket ( File Must be present locally, and it can take file_name/
                          file_path)
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return: Success - Returns True
                 Failure - Returns False
        """
        logger.debug(
            f"Uploading File: {file_name} to Bucket: {bucket_name} with path: {object_path} , MIME: {content_type} ")
        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.client.upload_file(file_name, bucket_name, object_path, ExtraArgs={'ContentType': content_type})
            return True
        else:
            logger.exception(f"Failed to Upload file to the Specified Bucket (upload_file)")
            return False

    def upload_file_and_get_link(self,
                                 bucket_name: str,
                                 file_name: str,
                                 object_path: str,
                                 content_type='octet/stream') -> str:
        """
        This function is used to upload file to specified bucket .
        You must have WRITE permissions on a bucket to add an object to it.
        @param content_type: MIME Type of the file.(By default it's considered as octet/stream)
        @param bucket_name: S3 Bucket Name
        @param file_name: File to be uploaded to S3 Bucket ( File Must be present locally, and it can take file_name/
                          file_path)
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return: Success - Returns Non Pre-signed Link
                 Failure - Returns Empty String
        """
        logger.debug(
            f"Uploading File: {file_name} to Bucket: {bucket_name} with path: {object_path} , MIME: {content_type} ")
        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.client.upload_file(file_name, bucket_name, object_path, ExtraArgs={'ContentType': content_type})
            return self.hostname + bucket_name + '/' + object_path
        else:
            logger.exception(f"Failed to Upload file to the Specified Bucket (upload_file_and_get_link)")
            return EMPTY_STRING

    def upload_file_and_get_pre_signed_link(self,
                                            bucket_name: str,
                                            file_name: str,
                                            object_path: str,
                                            expires_in=settings.DEFAULT_S3_LINK_EXPIRY_TIMEOUT,
                                            content_type='octet/stream',
                                            ) -> str:
        """
        This function is used to upload file to specified bucket and get pre-signed link.
        You must have WRITE permissions on a bucket to add an object to it.
        @param bucket_name: S3 Bucket Name
        @param file_name: File to be uploaded to S3 Bucket ( File Must be present locally, and it can take file_name/
                          file_path)
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @param content_type: MIME Type of the file.(By default it's considered as octet/stream)
        @param expires_in: Expiry Time in seconds
        @return:  Success - Returns Pre-signed Link
                  Failure - Returns Empty String
        """
        logger.debug(
            f"Uploading File: {file_name} to Bucket: {bucket_name} with path: {object_path} , MIME: {content_type} ")
        if self.check_bucket_exist(bucket_name) and self.check_local_file_exist(file_name):
            self.client.upload_file(file_name, bucket_name, object_path, ExtraArgs={'ContentType': content_type})
            return self.generate_pre_signed_link(bucket_name=bucket_name,
                                                 object_path=object_path,
                                                 expires_in=expires_in)
        else:
            logger.exception(f"Failed to Upload file to the Specified Bucket (upload_file_and_get_pre_signed_link)")
            return EMPTY_STRING

    def delete_object(self, bucket_name: str, object_path: str) -> bool:
        """
        This function is used to delete object from the specified bucket.
        @param bucket_name: S3 Bucket Name
        @param object_path: This is the path in S3 (inside Specified Bucket) where file needs to be stored.
                            e.x : object_path='temp/a.txt'  or object_path='a.txt'
        @return: Success - Returns True
                 Failure - Returns False
        """
        try:
            logger.warning(f"Deleting a File from Bucket: {bucket_name} with path: {object_path}")
            if self.check_bucket_exist(bucket_name) and self.check_object_exist(bucket_name, object_path):
                response = self.client.delete_object(Bucket=bucket_name, Key=object_path)
                return response['ResponseMetadata']['HTTPStatusCode'] == 204
            else:
                logger.debug(f"While Deleting File : Bucket/ File Doesn't Exists")
                return False
        except ClientError as e:
            logger.exception(f"Client Error Occured During Deleting File (delete_object) : {e}")
            return False
        except Exception as e:
            logger.exception(f"Unhandled Exception Occured During Deleting File (delete_object) : {e}")
            return False


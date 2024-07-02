import boto3
from base64 import b64decode
from dotenv import load_dotenv
import os


class AwsSettings:

    def __init__(self):
        load_dotenv()

    def get_decrypted_value_aws(self, value: str) -> str:
        return boto3.client('kms').decrypt(
            CiphertextBlob=b64decode(value),
            EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
        )['Plaintext'].decode('utf-8')

    def get_storage_password(self, storage_name: str) -> str:
        allowed_tokens = [True, 'true', 'True', '1', 1]
        return os.environ.get(storage_name) if os.environ.get("DEBUG") in allowed_tokens \
            else AwsSettings.get_decrypted_value_aws(os.environ.get(storage_name))


AwsSettings = AwsSettings()

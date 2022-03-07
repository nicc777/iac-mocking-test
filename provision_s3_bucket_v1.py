import traceback
import os
import sys
import boto3


AWS_REGION = os.getenv('AWS_REGION', 'eu-central-1')


def get_aws_client(boto3_library, service: str='s3', region: str=AWS_REGION):
    return boto3_library.client(service_name=service, region_name=region)


def get_s3_bucket_names(client)->list:
    bucket_names = list()
    response = client.list_buckets()
    for bucket in response['Buckets']:
        bucket_names.append(bucket['Name'])
    return bucket_names


def create_s3_bucket(client, bucket_name: str)->str:
    response = client.create_bucket(Bucket=bucket_name)
    return response['Location']


def main(
    target_bucket_name: str,
    boto3_lib=boto3
):
    bucket_names = get_s3_bucket_names(
        client=get_aws_client(boto3_library=boto3_lib, service='s3', region=AWS_REGION)
    )
    if target_bucket_name not in bucket_names:
        create_s3_bucket(
            client=get_aws_client(boto3_library=boto3_lib, service='s3', region=AWS_REGION),
            bucket_name=target_bucket_name
        )


if __name__ == '__main__':
    main(target_bucket_name=sys.argv[1], boto3_lib=boto3)


###############################################################################
###                                                                         ###
###                            U N I T T E S T S                            ###
###                                                                         ###
###############################################################################


import unittest
from datetime import datetime
from unittest.mock import patch


test_target_bucket_name = 'test_bucket'
responses = {
    'bucket_names_with_target_bucket_excluded': {
        'Buckets': [
            {
                'Name': 'example1',
                'CreationDate': datetime(2015, 1, 1)
            },
            {
                'Name': 'example2',
                'CreationDate': datetime(2015, 2, 2)
            },
        ],
        'Owner': {
            'DisplayName': 'ABC',
            'ID': 'ABC'
        }
    },
    'bucket_names_with_target_bucket_included': {
        'Buckets': [
            {
                'Name': 'example1',
                'CreationDate': datetime(2015, 1, 1)
            },
            {
                'Name': 'example2',
                'CreationDate': datetime(2015, 1, 1)
            },
            {
                'Name': test_target_bucket_name,
                'CreationDate': datetime(2015, 3, 3)
            },
        ],
        'Owner': {
            'DisplayName': 'ABC',
            'ID': 'ABC'
        }
    }
}



class MockS3Client:
    
    def __init__(
        self,
        list_bucket_response: dict=responses['bucket_names_with_target_bucket_excluded']
    ):
        self.list_buckets_response = list_bucket_response

    def list_buckets(self):
        return self.list_buckets_response

    def _bucket_name_exists(self, name: str)->bool:
        for bucket in self.list_buckets_response['Buckets']:
            if name == bucket['Name']:
                return True
        return False

    def create_bucket(self, *args, **kwargs):
        if 'Bucket' in kwargs:
            if self._bucket_name_exists(name=kwargs['Bucket']) is False:
                return {
                    'Location': 'test_location_response'
                }
        raise Exception('Call Failed')


class TestBoto3Functions(unittest.TestCase):

    def test_boto3_patching(self):
        with patch.object(boto3, 'client', return_value=MockS3Client()) as mock_method:
            client = boto3.client(service_name='s3', region_name='TEST_REGION')
            self.assertIsNotNone(client)
            self.assertIsInstance(client, MockS3Client)
            mock_method.assert_called_once_with(service_name='s3', region_name='TEST_REGION')

    def test_get_aws_client(self):
        with patch.object(boto3, 'client', return_value=MockS3Client()) as mock_method:
            client = get_aws_client(boto3_library=boto3, service='s3', region='TEST_REGION2')
            self.assertIsNotNone(client)
            self.assertIsInstance(client, MockS3Client)
            mock_method.assert_called_with(service_name='s3', region_name='TEST_REGION2')


class TestGetS3Buckets(unittest.TestCase):

    def test_get_s3_bucket_names_with_target_bucket_name_excluded(self):
        with patch.object(boto3, 'client', return_value=MockS3Client(list_bucket_response=responses['bucket_names_with_target_bucket_excluded'])) as mock_method:
            result = get_s3_bucket_names(client=get_aws_client(boto3_library=boto3, service='s3', region='TEST_REGION2'))
            self.assertIsNotNone(result)
            self.assertIsInstance(result, list)
            self.assertFalse(test_target_bucket_name in result)
            self.assertEqual(len(result), 2)
            self.assertTrue('example1' in result)
            self.assertTrue('example2' in result)

    def test_get_s3_bucket_names_with_target_bucket_name_included(self):
        with patch.object(boto3, 'client', return_value=MockS3Client(list_bucket_response=responses['bucket_names_with_target_bucket_included'])) as mock_method:
            result = get_s3_bucket_names(client=get_aws_client(boto3_library=boto3, service='s3', region='TEST_REGION2'))
            self.assertIsNotNone(result)
            self.assertIsInstance(result, list)
            self.assertTrue(test_target_bucket_name in result)
            self.assertEqual(len(result), 3)
            self.assertTrue('example1' in result)
            self.assertTrue('example2' in result)

    def test_create_s3_bucket_success(self):
        with patch.object(boto3, 'client', return_value=MockS3Client(list_bucket_response=responses['bucket_names_with_target_bucket_excluded'])) as mock_method:
            result = create_s3_bucket(client=get_aws_client(boto3_library=boto3, service='s3', region='TEST_REGION2'), bucket_name=test_target_bucket_name)
            self.assertIsNotNone(result)
            self.assertIsInstance(result, str)
            self.assertEqual(result, 'test_location_response')

    def test_create_s3_bucket_fail(self):
        with patch.object(boto3, 'client', return_value=MockS3Client(list_bucket_response=responses['bucket_names_with_target_bucket_included'])) as mock_method:
            with self.assertRaises(Exception) as context:
                create_s3_bucket(client=get_aws_client(boto3_library=boto3, service='s3', region='TEST_REGION2'), bucket_name=test_target_bucket_name)


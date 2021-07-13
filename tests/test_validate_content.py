import os
import unittest

import boto3
import pandas as pd

import orthoimagery_pipeline.common.config as config
from orthoimagery_pipeline.validate.validate import ValidateContent

CWD = os.path.abspath(os.getcwd())
print CWD
download_outputs = 'image_list.csv'
s3 = boto3.client('s3')


def clear_outputs(files):
    """
    :param files: list of output files to be cleared from disk
    :return: None
    """
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


class TestL7ValidateContent(unittest.TestCase):
    source = 'l7'

    @classmethod
    def setUpClass(cls):
        cls.val_content = ValidateContent.find_validation_strategy(CWD, cls.source)

    @classmethod
    def tearDownClass(cls):
        cls.val_content._clean_up()
        clear_outputs(cls.val_content.output_files)

    def test1_download_image_set(self):
        # assert no l8 files exist prior to function call
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Output file exists prior to downloading image set: %s' % needed_output)
        # call function
        self.val_content._download_image_set()

        # assert final output file exists
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output),
                            'File %s failed to download during _download_image_set' % needed_output)

    def test2_valid(self):

        # this function does unit tests for both validate bands and validate metadata

        self.assertEquals(len(self.val_content.download_url_val), 0, 'Valid URLs list not initialized to empty list')
        self.assertEquals(len(self.val_content.download_url_inval), 0,
                          'Invalid URLs list not initialized to empty list')

        self.val_content._validate_bands()
        self.val_content._validate_metadata()

        # assert valid and invalid download url lists are populated after calling _validate_bands
        num_val = len(self.val_content.download_url_val)
        num_inval = len(self.val_content.download_url_inval)

        ds = pd.read_csv(self.val_content.download_url_csv, header=None)
        ds.columns = ['download_url']

        num_expected = len(ds.download_url.unique())

        self.assertTrue(num_val + num_inval == num_expected,
                        'Count of valid/invalid images is not as expected, valid images is %s, invalid images is %s and total number of expected images are %s' % (
                            num_val, num_inval, num_expected))

    def test3_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.val_content._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.val_content.s3_validate_folder + config.download_url

        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            print file_key, before_keys, before_time
            print response_before, '\n' , response_after
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')


class TestL8ValidateContent(unittest.TestCase):
    source = 'l8'

    @classmethod
    def setUpClass(cls):
        cls.val_content = ValidateContent.find_validation_strategy(CWD, cls.source)

    @classmethod
    def tearDownClass(cls):
        cls.val_content._clean_up()
        clear_outputs(cls.val_content.output_files)

    def test1_download_image_set(self):
        # assert no l8 files exist prior to function call
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Output file exists prior to downloading image set: %s' % needed_output)
        # call function
        self.val_content._download_image_set()

        # assert final output file exists
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output),
                            'File %s failed to download during _download_image_set' % needed_output)

    def test2_valid(self):

        # this function does unit tests for both validate bands and validate metadata

        self.assertEquals(len(self.val_content.download_url_val), 0, 'Valid URLs list not initialized to empty list')
        self.assertEquals(len(self.val_content.download_url_inval), 0,
                          'Invalid URLs list not initialized to empty list')

        self.val_content._validate_bands()
        self.val_content._validate_metadata()

        # assert valid and invalid download url lists are populated after calling _validate_bands
        num_val = len(self.val_content.download_url_val)
        num_inval = len(self.val_content.download_url_inval)

        ds = pd.read_csv(self.val_content.download_url_csv, header=None)
        ds.columns = ['download_url']

        num_expected = len(ds.download_url.unique())

        self.assertTrue(num_val + num_inval == num_expected,
                        'Count of valid/invalid images is not as expected, valid images is %s, invalid images is %s and total number of expected images are %s' % (
                            num_val, num_inval, num_expected))

    def test3_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.val_content._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.val_content.s3_validate_folder + config.download_url

        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')


class TestSentinelValidateContent(unittest.TestCase):
    source = 'l8'

    @classmethod
    def setUpClass(cls):
        cls.val_content = ValidateContent.find_validation_strategy(CWD, cls.source)

    @classmethod
    def tearDownClass(cls):
        cls.val_content._clean_up()
        clear_outputs(cls.val_content.output_files)

    def test1_download_image_set(self):
        # assert no l8 files exist prior to function call
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Output file exists prior to downloading image set: %s' % needed_output)
        # call function
        self.val_content._download_image_set()

        # assert final output file exists
        for needed_output in self.val_content.output_files:
            self.assertTrue(os.path.isfile(needed_output),
                            'File %s failed to download during _download_image_set' % needed_output)

    def test2_valid(self):

        # this function does unit tests for both validate bands and validate metadata

        self.assertEquals(len(self.val_content.download_url_val), 0, 'Valid URLs list not initialized to empty list')
        self.assertEquals(len(self.val_content.download_url_inval), 0,
                          'Invalid URLs list not initialized to empty list')

        self.val_content._validate_bands()
        self.val_content._validate_metadata()

        # assert valid and invalid download url lists are populated after calling _validate_bands
        num_val = len(self.val_content.download_url_val)
        num_inval = len(self.val_content.download_url_inval)

        ds = pd.read_csv(self.val_content.download_url_csv, header=None)
        ds.columns = ['download_url']

        num_expected = len(ds.download_url.unique())

        self.assertTrue(num_val + num_inval == num_expected,
                        'Count of valid/invalid images is not as expected, valid images is %s, invalid images is %s and total number of expected images are %s' % (
                            num_val, num_inval, num_expected))

    def test3_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.val_content._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.val_content.s3_validate_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.val_content.s3_validate_folder + config.download_url

        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')


if __name__ == "__main__":
    unittest.main()

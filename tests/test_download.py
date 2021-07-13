import os
import re
import shutil
import unittest
from glob import glob

import boto3

import orthoimagery_pipeline.common.config as config
from orthoimagery_pipeline.download.download import DownloadContent

CWD = os.path.abspath(os.getcwd())
file_dir = os.path.abspath(os.path.dirname(__file__))
fixtures_dir = os.path.join(file_dir, 'fixtures')
s3 = boto3.client('s3')


def clear_outputs(files):
    """
    :param files: list of outpu files to be cleared from disk
    :return: None
    """
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


class TestL7Download(unittest.TestCase):
    source = 'l7'
    fixture_file = os.path.join(fixtures_dir, 'image_list_l7_test.csv')
    band_regex = re.compile('(B[1-5].TIF)|(B6_VCID_[1-2].TIF)|(B[7-8].TIF)')
    dl_dir = config.L7_DATA_FOLDER

    @classmethod
    def setUpClass(cls):
        cls.download = DownloadContent.download_content(cls.source, CWD)

        # Replace the validate output file in S3 with doctored one
        # from fixtures folder to control number of downloaded files
        s3.upload_file(cls.fixture_file, config.OUTPUT_BUCKET, cls.download.s3_validate_folder + config.download_url,
                       ExtraArgs={'ServerSideEncryption': "AES256"})

        if os.path.exists(cls.dl_dir):
            shutil.rmtree(cls.dl_dir)

    @classmethod
    def tearDownClass(cls):
        cls.download._clean_up()
        clear_outputs(cls.download.output_files)
        os.chdir(CWD)

    def test1_download_image_set(self):
        self.assertFalse(os.path.exists(self.download.download_url_csv),
                         'Image set file %s already exists' % self.download.download_url_csv)

        self.download._download_image_set()

        self.assertTrue(os.path.exists(self.download.download_url_csv),
                        'Image set file %s failed to download' % self.download.download_url_csv)

    def test2_download_tif(self):
        self.assertFalse(os.path.exists(self.dl_dir), 'The data directory %s already exists' % self.dl_dir)

        self.download._download_tif()

        self.assertTrue(self.dl_dir, 'The data directory %s failed to create' % self.dl_dir)

        with open(self.download.download_url_csv, 'r') as valid_image_list:
            lines = valid_image_list.readlines()
            num_expected_dls = len(lines)

        downloads = os.listdir(self.dl_dir)
        self.assertEquals(num_expected_dls, len(downloads),
                          'The number of downloads was not as expected. Expected: %d Found %d' % (
                              num_expected_dls, len(downloads)))

        tifs_per_dl = 9
        for dl in downloads:
            image_dir = os.path.join(self.dl_dir, dl)
            os.chdir(image_dir)
            tifs = [t for t in glob("*.TIF") if self.band_regex.search(t)]
            self.assertEquals(len(tifs), tifs_per_dl,
                              "Number of tifs downloaded for %s does not include the expected %d. Found: %d" % (
                                  dl, tifs_per_dl, len(tifs)))


class TestL8Download(unittest.TestCase):
    source = 'l8'
    fixture_file = os.path.join(fixtures_dir, 'image_list_l8_test.csv')
    band_regex = re.compile('B([1-7]|9|1[0-1]).TIF$')
    dl_dir = config.L8_DATA_FOLDER

    @classmethod
    def setUpClass(cls):
        cls.download = DownloadContent.download_content(cls.source, CWD)
        # Replace the validate output file in S3 with doctored one
        # from fixtures folder to control number of downloaded files
        s3.upload_file(cls.fixture_file, config.OUTPUT_BUCKET, cls.download.s3_validate_folder + config.download_url,
                       ExtraArgs={'ServerSideEncryption': "AES256"})

        if os.path.exists(cls.dl_dir):
            shutil.rmtree(cls.dl_dir)

    @classmethod
    def tearDownClass(cls):
        cls.download._clean_up()
        clear_outputs(cls.download.output_files)
        os.chdir(CWD)

    def test1_download_image_set(self):
        self.assertFalse(os.path.exists(self.download.download_url_csv),
                         'Image set file %s already exists' % self.download.download_url_csv)

        self.download._download_image_set()

        self.assertTrue(os.path.exists(self.download.download_url_csv),
                        'Image set file %s failed to download' % self.download.download_url_csv)

    def test2_download_tif(self):
        self.assertFalse(os.path.exists(self.dl_dir), 'The data directory %s already exists' % self.dl_dir)

        self.download._download_tif()

        self.assertTrue(os.path.exists(self.dl_dir), 'The data directory %s failed to create' % self.dl_dir)

        with open(self.download.download_url_csv, 'r') as valid_image_list:
            lines = valid_image_list.readlines()
            num_expected_dls = len(lines)

        downloads = os.listdir(self.dl_dir)
        self.assertEquals(num_expected_dls, len(downloads),
                          'The number of downloads was not as expected. Expected: %d Found %d' % (
                              num_expected_dls, len(downloads)))

        tifs_per_dl = 10
        for dl in downloads:
            image_dir = os.path.join(self.dl_dir, dl)
            os.chdir(image_dir)
            tifs = [t for t in glob("*.TIF") if self.band_regex.search(t)]
            self.assertEquals(len(tifs), tifs_per_dl,
                              "Number of tifs downloaded for %s is not the expected %d. Found: %d" % (
                                  dl, tifs_per_dl, len(tifs)))


class TestSentinelDownload(unittest.TestCase):
    source = 'sentinel'
    fixture_file = os.path.join(fixtures_dir, 'image_list_sentinel_test.csv')
    band_regex = re.compile('(B0[1-8].jp2)|(B8A.jp2)|(B09.jp2)|(B1[0-2].jp2)')
    dl_dir = config.SENTINEL_DATA_FOLDER

    @classmethod
    def setUpClass(cls):
        cls.download = DownloadContent.download_content(cls.source, CWD)

        # Replace the validate output file in S3 with doctored one
        # from fixtures folder to control number of downloaded files
        s3.upload_file(cls.fixture_file, config.OUTPUT_BUCKET, cls.download.s3_validate_folder + config.download_url,
                       ExtraArgs={'ServerSideEncryption': "AES256"})

        if os.path.exists(cls.dl_dir):
            shutil.rmtree(cls.dl_dir)

    @classmethod
    def tearDownClass(cls):
        cls.download._clean_up()
        clear_outputs(cls.download.output_files)
        os.chdir(CWD)

    def test1_download_image_set(self):
        self.assertFalse(os.path.exists(self.download.download_url_csv),
                         'Image set file %s already exists' % self.download.download_url_csv)

        self.download._download_image_set()

        self.assertTrue(os.path.exists(self.download.download_url_csv),
                        'Image set file %s failed to download' % self.download.download_url_csv)

    def test2_download_tif(self):
        self.assertFalse(os.path.exists(self.dl_dir), 'The data directory %s already exists' % self.dl_dir)

        self.download._download_tif()

        self.assertTrue(os.path.exists(self.dl_dir), 'The data directory %s failed to create' % self.dl_dir)

        with open(self.download.download_url_csv, 'r') as valid_image_list:
            lines = valid_image_list.readlines()
            num_expected_dls = len(lines)

        granule_dirs = [d[0] for d in os.walk(self.dl_dir) if 'GRANULE' in d[0] and 'IMG_DATA' in d[0]]
        self.assertEquals(num_expected_dls, len(granule_dirs),
                          'The number of downloads was not as expected. Expected: %d Found %d' % (
                              num_expected_dls, len(granule_dirs)))

        jp2s_per_dl = 13
        for dl in granule_dirs:
            image_dir = os.path.join(self.dl_dir, dl)
            os.chdir(image_dir)
            jp2s = [j for j in glob("*.jp2") if self.band_regex.search(j)]
            self.assertEquals(len(jp2s), jp2s_per_dl,
                              "Number of tifs downloaded for %s is not the expected %d. Found: %d" % (
                                  dl, jp2s_per_dl, len(jp2s)))


if __name__ == "__main__":
    unittest.main()

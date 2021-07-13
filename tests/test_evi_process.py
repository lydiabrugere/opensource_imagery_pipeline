import os
import unittest

import boto3

import orthoimagery_pipeline.common.config as config
from orthoimagery_pipeline.evi_process.evi_process import EVIProcess
from orthoimagery_pipeline.evi_process.evi_process import to_delete

s3 = boto3.client('s3')

REF_SUFFIX = 'ref.img'
THERMAL_SUFFIX = 'thermal.img'
ANGLES_SUFFIX = 'angles.img'
SATURATION_SUFFIX = 'saturationmask.img'
TOA_SUFFIX = 'toa.img'
CLOUD_SUFFIX = 'cloud.img'

WGS_SUFFIX = '_wgs84.tif'
MASKED_SUFFIX = '_masked.tif'
TILED_SUFFIX = '_tiled.tif'

IMG_SUFFICES = {REF_SUFFIX, THERMAL_SUFFIX, ANGLES_SUFFIX, SATURATION_SUFFIX, TOA_SUFFIX, CLOUD_SUFFIX}
GEOPROCESS_SUFFICES = {WGS_SUFFIX, MASKED_SUFFIX, TILED_SUFFIX}


def is_img_suffixed_file(filename):
    filename_parts = filename.split(os.sep)
    filename = filename_parts[-1]

    return filename in IMG_SUFFICES


def is_geoprocessed_file(filename):
    for suffix in GEOPROCESS_SUFFICES:
        if filename.endswith(suffix):
            return True
    return False


class TestL8EVIProcess(unittest.TestCase):
    source = 'l8'
    EVI_suffix = '_EVI.tif'
    s3_prefix = config.EVI_PREFIX + 'l8/'

    @classmethod
    def setUpClass(cls):
        cls.evi_process = EVIProcess.evi_process(cls.source)

    def test0_correct_data_dirs_populated(self):

        expected_dirs = [os.path.join(config.L8_DATA_FOLDER, dir_member) for dir_member in
                         os.listdir(config.L8_DATA_FOLDER)
                         if os.path.isdir(os.path.join(config.L8_DATA_FOLDER, dir_member))]

        observered_dirs = [data_dir for data_dir in self.evi_process.sub_dirs]

        self.assertTrue(len(observered_dirs) > 0, 'Number of initial data directories is zero.')

        for observed_dir in observered_dirs:
            self.assertTrue(observed_dir in expected_dirs,
                            'Data directory %s is missing from the expected directories:\n %s' %
                            (observed_dir, '\n'.join(expected_dirs)))

    def test1_cloud_mask(self):

        # get all files in data dir prior to tests and assert cloud mask output doesn't already exist
        files_before_cm = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        self.assertTrue(
            all([not is_img_suffixed_file(f) for f in files_before_cm]),
            'Existing .img files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cloud_mask()

        files_after_cm = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        for suffix in IMG_SUFFICES:
            num_imgs = len([f for f in files_after_cm if f.endswith(suffix)])
            self.assertEquals(num_imgs, num_expected_refs,
                              'Found %d img files with suffix %s, while the excepted was %d' % (
                                  num_imgs, suffix, num_expected_refs))

    def test2_cal_evi(self):

        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_cal = [os.path.join(data_dir, found_file)
                            for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                            for found_file in found_files]

        self.assertTrue(
            all([not f.endswith(self.EVI_suffix) for f in files_before_cal]),
            'Existing .EVI tif files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cal_evi()

        files_after_cal = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        num_tifs = len([f for f in files_after_cal if f.endswith(self.EVI_suffix)])

        self.assertEquals(num_tifs, num_expected_refs,
                          'Found %d tif files with suffix %s, while the excepted was %d' % (
                              num_tifs, self.EVI_suffix, num_expected_refs))

    def test3_geoprocess(self):

        # call cloud mask method
        self.evi_process._geoprocess()

        files_after_gp = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_tifs = len(self.evi_process.sub_dirs) * len(GEOPROCESS_SUFFICES)
        num_tifs = len([f for f in files_after_gp if is_geoprocessed_file(f)])

        self.assertGreater(num_tifs, 0, 'No geoprocessing tif files found.')
        self.assertEquals(num_tifs, num_expected_tifs,
                          'Found %d geoprocessing tif files, while the excepted was %d' % (num_tifs, num_expected_tifs))

    def test4_test_clean_up(self):
        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_clean = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                              for found_file in found_files]

        num_to_delete = len([f for f in files_before_clean if to_delete(f)])

        self.assertTrue(num_to_delete > 0, 'No output files found for deletion')

        # call cloud mask method
        self.evi_process._clean_up()

        files_after_clean = [os.path.join(data_dir, found_file)
                             for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                             for found_file in found_files]

        num_expected = len(files_before_clean) - num_to_delete
        num_observed = len(files_after_clean)

        self.assertEquals(num_expected, num_observed,
                          'Number of deleted files in clean up was %d, while expected was %d' % (
                              num_observed, num_expected))

    def test5_img_to_gs_pub(self):

        files_before_gs_pub = [os.path.join(data_dir, found_file)
                               for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                               for found_file in found_files]

        self.evi_process._img_to_gs_pub()

        files_after_gs_pub = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                              for found_file in found_files]

        expected_additions = len(self.evi_process.sub_dirs) * 3  # x3 is for .tif, .aux.xml, and .over
        observed_additions = len(files_after_gs_pub) - len(files_before_gs_pub)

        self.assertEquals(observed_additions, expected_additions,
                          'Number of processed tif files added to EFS was: %d. Expected %d' % (
                              observed_additions, expected_additions))

    def test6_img_to_s3(self):

        # currently tests that all subdir data is synced to s3
        # test might change if data longevity requirements change

        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.evi_process._img_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )
        after_keys = [elem['Key'] for elem in response_after['Contents']]

        upload_data = [os.path.join(data_dir, found_file)
                       for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                       for found_file in found_files]

        observed_uploads = len(after_keys) - len(before_keys)
        expected_uploads = len(upload_data)

        self.assertEquals(observed_uploads, expected_uploads,
                          'Number of observed uploads was: %d. Expected %d.' % (observed_uploads, expected_uploads))

    def test7_efs2_clean(self):

        self.evi_process._efs2_clean()

        folders_after_efs2_clean = [data_dir for _, data_dir, _ in os.walk(self.evi_process.base_dir)]
        num_folders = len(folders_after_efs2_clean[0])

        self.assertEquals(num_folders, 0, 'Number of scene folders on processing EFS was: %d. Expected: 0' % (num_folders))


class TestL7EVIProcess(unittest.TestCase):
    source = 'l7'
    EVI_suffix = '_EVI.tif'
    s3_prefix = config.EVI_PREFIX + 'l7/'

    @classmethod
    def setUpClass(cls):
        cls.evi_process = EVIProcess.evi_process(cls.source)

    def test0_correct_data_dirs_populated(self):

        expected_dirs = [os.path.join(config.L7_DATA_FOLDER, dir_member) for dir_member in
                         os.listdir(config.L7_DATA_FOLDER)
                         if os.path.isdir(os.path.join(config.L7_DATA_FOLDER, dir_member))]

        observered_dirs = [data_dir for data_dir in self.evi_process.sub_dirs]

        self.assertTrue(len(observered_dirs) > 0, 'Number of initial data directories is zero.')

        for observed_dir in observered_dirs:
            self.assertTrue(observed_dir in expected_dirs,
                            'Data directory %s is missing from the expected directories:\n %s' %
                            (observed_dir, '\n'.join(expected_dirs)))

    def test1_cloud_mask(self):

        # get all files in data dir prior to tests and assert cloud mask output doesn't already exist
        files_before_cm = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        self.assertTrue(
            all([not is_img_suffixed_file(f) for f in files_before_cm]),
            'Existing .img files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cloud_mask()

        files_after_cm = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        for suffix in IMG_SUFFICES:
            num_imgs = len([f for f in files_after_cm if f.endswith(suffix)])
            self.assertEquals(num_imgs, num_expected_refs,
                              'Found %d img files with suffix %s, while the excepted was %d' % (
                                  num_imgs, suffix, num_expected_refs))

    def test2_cal_evi(self):

        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_cal = [os.path.join(data_dir, found_file)
                            for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                            for found_file in found_files]

        self.assertTrue(
            all([not f.endswith(self.EVI_suffix) for f in files_before_cal]),
            'Existing .EVI tif files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cal_evi()

        files_after_cal = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        num_tifs = len([f for f in files_after_cal if f.endswith(self.EVI_suffix)])

        self.assertEquals(num_tifs, num_expected_refs,
                          'Found %d tif files with suffix %s, while the excepted was %d' % (
                              num_tifs, self.EVI_suffix, num_expected_refs))

    def test3_geoprocess(self):

        # call cloud mask method
        self.evi_process._geoprocess()

        files_after_gp = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_tifs = len(self.evi_process.sub_dirs) * len(GEOPROCESS_SUFFICES)
        num_tifs = len([f for f in files_after_gp if is_geoprocessed_file(f)])

        self.assertGreater(num_tifs, 0, 'No geoprocessing tif files found.')
        self.assertEquals(num_tifs, num_expected_tifs,
                          'Found %d geoprocessing tif files, while the excepted was %d' % (num_tifs, num_expected_tifs))

    def test4_test_clean_up(self):
        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_clean = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                              for found_file in found_files]

        num_to_delete = len([f for f in files_before_clean if to_delete(f)])

        self.assertTrue(num_to_delete > 0, 'No output files found for deletion')

        # call cloud mask method
        self.evi_process._clean_up()

        files_after_clean = [os.path.join(data_dir, found_file)
                             for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                             for found_file in found_files]

        num_expected = len(files_before_clean) - num_to_delete
        num_observed = len(files_after_clean)

        self.assertEquals(num_expected, num_observed,
                          'Number of deleted files in clean up was %d, while expected was %d' % (
                              num_observed, num_expected))

    def test5_img_to_gs_pub(self):

        files_before_gs_pub = [os.path.join(data_dir, found_file)
                               for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                               for found_file in found_files]

        self.evi_process._img_to_gs_pub()

        files_after_gs_pub = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                              for found_file in found_files]

        expected_additions = len(self.evi_process.sub_dirs) * 3  # x3 is for .tif, .aux.xml, and .over
        observed_additions = len(files_after_gs_pub) - len(files_before_gs_pub)

        self.assertEquals(observed_additions, expected_additions,
                          'Number of processed tif files added to EFS was: %d. Expected %d' % (
                              observed_additions, expected_additions))

    def test6_img_to_s3(self):

        # currently tests that all subdir data is synced to s3
        # test might change if data longevity requirements change

        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.evi_process._img_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )
        after_keys = [elem['Key'] for elem in response_after['Contents']]

        upload_data = [os.path.join(data_dir, found_file)
                       for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                       for found_file in found_files]

        observed_uploads = len(after_keys) - len(before_keys)
        expected_uploads = len(upload_data)

        self.assertEquals(observed_uploads, expected_uploads,
                          'Number of expected uploads was: %d. Expected %d.' % (observed_uploads, expected_uploads))


class TestSentinelEVIProcess(unittest.TestCase):
    source = 'sentinel'
    EVI_suffix = '_EVI.tif'
    s3_prefix = config.EVI_PREFIX + 'sentinel/'

    @classmethod
    def setUpClass(cls):
        cls.evi_process = EVIProcess.evi_process(cls.source)

    def test0_correct_data_dirs_populated(self):

        expected_dirs = [os.path.join(config.SENTINEL_DATA_FOLDER, dir_member)
                         for dir_member in os.listdir(config.SENTINEL_DATA_FOLDER)
                         if os.path.isdir(os.path.join(config.SENTINEL_DATA_FOLDER, dir_member))]

        observed_dirs = [data_dir for data_dir in self.evi_process.sub_dirs]

        self.assertTrue(len(observed_dirs) > 0, 'Number of initial data directories is zero.')

        for observed_dir in observed_dirs:
            self.assertTrue(observed_dir in expected_dirs,
                            'Data directory %s is missing from the expected directories:\n %s' %
                            (observed_dir, '\n'.join(expected_dirs)))

    def test1_cloud_mask(self):

        # get all files in data dir prior to tests and assert cloud mask output doesn't already exist
        files_before_cm = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        self.assertTrue(
            all([not is_img_suffixed_file(f) for f in files_before_cm]),
            'Existing .img files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cloud_mask()

        files_after_cm = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        for suffix in ('angles.img', 'cloud.img'):
            num_imgs = len([f for f in files_after_cm if f.endswith(suffix)])
            self.assertEquals(num_imgs, num_expected_refs,
                              'Found %d img files with suffix %s, while the excepted was %d' % (
                                  num_imgs, suffix, num_expected_refs))

    def test2_cal_evi(self):

        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_cal = [os.path.join(data_dir, found_file)
                            for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                            for found_file in found_files]

        self.assertTrue(
            all([not f.endswith(self.EVI_suffix) for f in files_before_cal]),
            'Existing .EVI tif files found in data directories. '
            'Tests require a clean start - no residual output allowed.'
        )

        # call cloud mask method
        self.evi_process._cal_evi()

        files_after_cal = [os.path.join(data_dir, found_file)
                           for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                           for found_file in found_files]

        num_expected_refs = len(self.evi_process.sub_dirs)

        num_tifs = len([f for f in files_after_cal if f.endswith(self.EVI_suffix)])

        self.assertEquals(num_tifs, num_expected_refs,
                          'Found %d tif files with suffix %s, while the excepted was %d' % (
                              num_tifs, self.EVI_suffix, num_expected_refs))

    def test3_geoprocess(self):

        # call cloud mask method
        self.evi_process._geoprocess()

        files_after_gp = [os.path.join(data_dir, found_file)
                          for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                          for found_file in found_files]

        num_expected_tifs = len(self.evi_process.sub_dirs) * len(GEOPROCESS_SUFFICES)
        num_tifs = len([f for f in files_after_gp if is_geoprocessed_file(f)])

        self.assertGreater(num_tifs, 0, 'No geoprocessing tif files found.')
        self.assertEquals(num_tifs, num_expected_tifs,
                          'Found %d geoprocessing tif files, while the excepted was %d' % (num_tifs, num_expected_tifs))

    def test4_test_clean_up(self):
        # get all files in data dir prior to tests and assert cal output doesn't already exist
        files_before_clean = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                              for found_file in found_files]

        num_to_delete = len([f for f in files_before_clean if to_delete(f)])

        self.assertTrue(num_to_delete > 0, 'No output files found for deletion')

        # call cloud mask method
        self.evi_process._clean_up()

        files_after_clean = [os.path.join(data_dir, found_file)
                             for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                             for found_file in found_files]

        num_expected = len(files_before_clean) - num_to_delete
        num_observed = len(files_after_clean)

        self.assertEquals(num_expected, num_observed,
                          'Number of deleted files in clean up was %d, while expected was %d' % (
                              num_observed, num_expected))

    def test5_img_to_gs_pub(self):

        files_before_gs_pub = [os.path.join(data_dir, found_file)
                               for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                               for found_file in found_files]

        self.evi_process._img_to_gs_pub()

        files_after_gs_pub = [os.path.join(data_dir, found_file)
                              for data_dir, _, found_files in os.walk(self.evi_process.pub_dir)
                              for found_file in found_files]

        expected_additions = len(self.evi_process.sub_dirs) * 3  # x3 is for .tif, .aux.xml, and .over
        observed_additions = len(files_after_gs_pub) - len(files_before_gs_pub)

        self.assertEquals(observed_additions, expected_additions,
                          'Number of processed tif files added to EFS was: %d. Expected %d' % (
                              observed_additions, expected_additions))

    def test6_img_to_s3(self):

        # currently tests that all subdir data is synced to s3
        # test might change if data longevity requirements change

        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.evi_process._img_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.s3_prefix
        )
        after_keys = [elem['Key'] for elem in response_after['Contents']]

        upload_data = [os.path.join(data_dir, found_file)
                       for data_dir, _, found_files in os.walk(self.evi_process.base_dir)
                       for found_file in found_files]

        observed_uploads = len(after_keys) - len(before_keys)
        expected_uploads = len(upload_data)

        self.assertEquals(observed_uploads, expected_uploads,
                          'Number of expected uploads was: %d. Expected %d.' % (observed_uploads, expected_uploads))


if __name__ == "__main__":
    unittest.main()

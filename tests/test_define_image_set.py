import os
import unittest
from datetime import datetime
from datetime import timedelta

import boto3
import psycopg2

import orthoimagery_pipeline.common.config as config
from orthoimagery_pipeline.define.define import DefineImageSet

CWD = os.path.abspath(os.getcwd())
END_DATE = datetime.utcnow()
START_DATE = END_DATE - timedelta(days=3)

DATE_FMT = '%Y-%m-%d'

# test for a continent
# continent = 'SA'
# l0_iso_code = None

# test for countries
continent = None
l0_iso_code = 'RO', 'DE'

s3 = boto3.client('s3')


def clear_outputs(files):
    """
    :param files: list of output files to be cleared from disk
    :return: None
    """
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


def get_tables(conn):
    """
    Helper function to run a query using PG connection
    :param conn: psycopg2 db connection
    :return: results of query
    """

    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='imagery';")
    results = cursor.fetchall()
    cursor.close()

    return results


class TestL7DefineImageSet(unittest.TestCase):
    source = 'l7'
    download_outputs = ('index.csv.gz', 'index.csv', 'google_l7_index.csv')
    scene_list_table = 'google_l7_scene_list'
    download_url_table = 'download_url'
    des_aoi_table = 'des_aoi'
    des_aoi_grid_table = 'des_aoi_grid'
    tables = ('download_url', 'google_l7_scene_list', 'des_aoi', 'des_aoi_grid')

    @classmethod
    def setUpClass(cls):
        cls.image_set = DefineImageSet.define_image_set(cls.source, CWD, START_DATE.strftime(DATE_FMT),
                                                        END_DATE.strftime(DATE_FMT), continent, l0_iso_code)
        cls.image_set._open_connection()  # open pg connection

    @classmethod
    def tearDownClass(cls):
        cls.image_set._clean_up()  # clear files in disk
        clear_outputs(cls.download_outputs)  # not needed?

    def test0_open_connection(self):
        self.assertTrue(type(self.image_set.conn) == psycopg2.extensions.connection,
                        'DefineImageSet instance is missing correct connection type')
        self.assertTrue(self.image_set.conn.closed == 0,
                        'DefineImageSet instance has closed connection. Should be open.')

    def test1_download_scene_list(self):
        # assert no l7 files exist prior to function call
        for needed_output in self.download_outputs:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Scene list output already exists: %s' % needed_output)

        # call function
        self.image_set._download_scene_list()

        # assert final output file exists
        self.assertTrue(os.path.isfile(self.download_outputs[-1]),
                        '_download_scene_list final output is missing: %s' % self.download_outputs[-1])

    def test2_create_scene_list_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table not in all_tables_before,
                        'Scene list table exists prior to call to _create_scene_list_table')

        # function call
        self.image_set._create_scene_list_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table in all_tables_after, 'Scene list table failed to create: %s')

    def test3_create_aoi_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table not in all_tables_before,
                        'Desired AOI table exists prior to call to _create_des_aoi_table: %s' % self.des_aoi_table)
        # function call
        self.image_set._create_des_aoi_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table in all_tables_after, 'Desired AOI table failed to create')

    def test4__create_sat_grid(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table not in all_tables_before,
                        'Desired AOI grid table exists prior to call to _create_download_url: %s' % self.des_aoi_grid_table)

        # function call
        self.image_set._create_sat_grid()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table in all_tables_after, 'Desired AOI grid table failed to create')

    def test5_create_download_url(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table not in all_tables_before,
                        'Download url table exists prior to call to _create_download_url: %s' % self.download_url_table)

        # function call
        self.image_set._create_download_url()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table in all_tables_after, 'Download url table failed to create')

    def test6_save_url_csv(self):
        # assert file does not exist yet
        self.assertTrue(not os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv file already exists: %s' % self.image_set.download_url_csv)

        # function call
        self.image_set._save_url_csv()

        # assert file now exists
        self.assertTrue(os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv failed to create: %s' % self.image_set.download_url_csv)

    def test7_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.image_set._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.image_set.s3_define_folder + config.download_url
        print file_key
        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')

    def test8_remove_db_tables(self):

        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]

        # assert tables exist before remove
        for table in self.tables:
            self.assertTrue(table in all_tables_before, 'Table does not exist: %s' % table)

        # function call
        self.image_set._remove_db_tables()

        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]

        # assert table no longer exist
        for table in self.tables:
            self.assertTrue(table not in all_tables_after, 'Table failed to delete: %s' % table)

    def test9_close_conn(self):
        # assert connection not yet closed
        self.assertTrue(self.image_set.conn.closed == 0, 'Connection not closed')

        # close connection
        self.image_set.conn.close()

        # assert connection is closed
        self.assertTrue(self.image_set.conn.closed != 0, 'Connection failed to close')

class TestL8DefineImageSet(unittest.TestCase):
    source = 'l8'
    download_outputs = ('scene_list.gz', 'scene_list', 'aws_l8_scene_list')
    scene_list_table = 'aws_l8_scene_list'
    download_url_table = 'download_url'
    des_aoi_table = 'des_aoi'
    des_aoi_grid_table = 'des_aoi_grid'
    # TODO: Why is tables defined twice?
    tables = tables = ('download_url', 'aws_l8_scene_list', 'des_aoi', 'des_aoi_grid')

    @classmethod
    def setUpClass(cls):
        cls.image_set = DefineImageSet.define_image_set(cls.source, CWD, START_DATE.strftime(DATE_FMT),
                                                        END_DATE.strftime(DATE_FMT), continent, l0_iso_code)
        cls.image_set._open_connection()

    @classmethod
    def tearDownClass(cls):
        cls.image_set._clean_up()
        clear_outputs(cls.download_outputs)

    def test0_open_connection(self):
        self.assertTrue(type(self.image_set.conn) == psycopg2.extensions.connection,
                        'DefineImageSet instance is missing correct connection type')
        self.assertTrue(self.image_set.conn.closed == 0,
                        'DefineImageSet instance has closed connection. Should be open.')

    def test1_download_scene_list(self):
        # assert no l7 files exist prior to function call
        for needed_output in self.download_outputs:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Scene list output already exists: %s' % needed_output)

        # call function
        self.image_set._download_scene_list()

        # assert final output file exists
        self.assertTrue(os.path.isfile(self.download_outputs[-1]),
                        '_download_scene_list final output is missing: %s' % self.download_outputs[-1])

    def test2_create_scene_list_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table not in all_tables_before,
                        'Scene list table exists prior to call to _create_scene_list_table')

        # function call
        self.image_set._create_scene_list_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table in all_tables_after, 'Scene list table failed to create: %s')

    def test3_create_aoi_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table not in all_tables_before,
                        'Desired AOI table exists prior to call to _create_des_aoi_table: %s' % self.des_aoi_table)
        # function call
        self.image_set._create_des_aoi_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table in all_tables_after, 'Desired AOI table failed to create')

    def test4__create_sat_grid(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table not in all_tables_before,
                        'Desired AOI grid table exists prior to call to _create_download_url: %s' % self.des_aoi_grid_table)

        # function call
        self.image_set._create_sat_grid()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table in all_tables_after, 'Desired AOI grid table failed to create')

    def test5_create_download_url(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table not in all_tables_before,
                        'Download url table exists prior to call to _create_download_url: %s' % self.download_url_table)

        # function call
        self.image_set._create_download_url()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table in all_tables_after, 'Download url table failed to create')

    def test6_save_url_csv(self):
        # assert file does not exist yet
        self.assertTrue(not os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv file already exists: %s' % self.image_set.download_url_csv)

        # function call
        self.image_set._save_url_csv()

        # assert file now exists
        self.assertTrue(os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv failed to create: %s' % self.image_set.download_url_csv)

    def test7_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.image_set._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.image_set.s3_define_folder + config.download_url
        print file_key

        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')

    def test8_remove_db_tables(self):

        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]

        # assert tables exist before remove
        for table in self.tables:
            self.assertTrue(table in all_tables_before, 'Table does not exist: %s' % table)

        # function call
        self.image_set._remove_db_tables()

        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]

        # assert table no longer exist
        for table in self.tables:
            self.assertTrue(table not in all_tables_after, 'Table failed to delete: %s' % table)

    def test9_close_conn(self):
        # assert connection not yet closed
        self.assertTrue(self.image_set.conn.closed == 0, 'Connection not closed')

        # close connection
        self.image_set.conn.close()

        # assert connection is closed
        self.assertTrue(self.image_set.conn.closed != 0, 'Connection failed to close')

class TestSentinelDefineImageSet(unittest.TestCase):
    source = 'sentinel'
    download_outputs = ('index.csv.gz', 'index.csv', 'google_sentinel2_index.csv')
    scene_list_table = 'google_sentinel2_index'
    download_url_table = 'download_url'
    des_aoi_table = 'des_aoi'
    des_aoi_grid_table = 'des_aoi_grid'
    tables = ('download_url', 'google_sentinel2_index', 'des_aoi', 'des_aoi_grid')

    @classmethod
    def setUpClass(cls):
        cls.image_set = DefineImageSet.define_image_set(cls.source, CWD, START_DATE.strftime(DATE_FMT),
                                                        END_DATE.strftime(DATE_FMT), continent, l0_iso_code)
        cls.image_set._open_connection()

    @classmethod
    def tearDownClass(cls):
        cls.image_set._clean_up()
        clear_outputs(cls.download_outputs)

    def test0_open_connection(self):
        self.assertTrue(type(self.image_set.conn) == psycopg2.extensions.connection,
                        'DefineImageSet instance is missing correct connection type')
        self.assertTrue(self.image_set.conn.closed == 0,
                        'DefineImageSet instance has closed connection. Should be open.')

    def test1_download_scene_list(self):
        # assert no sentinel files exist prior to function call
        for needed_output in self.download_outputs:
            self.assertTrue(os.path.isfile(needed_output) is False,
                            'Scene list output already exists: %s' % needed_output)

        # call function
        self.image_set._download_scene_list()

        # assert final output file exists
        self.assertTrue(os.path.isfile(self.download_outputs[-1]),
                        '_download_scene_list final output is missing: %s' % self.download_outputs[-1])

    def test2_create_scene_list_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table not in all_tables_before,
                        'Scene list table exists prior to call to _create_scene_list_table')

        # function call
        self.image_set._create_scene_list_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.scene_list_table in all_tables_after, 'Scene list table failed to create: %s')

    def test3_create_aoi_table(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table not in all_tables_before,
                        'Desired AOI table exists prior to call to _create_des_aoi_table: %s' % self.des_aoi_table)
        # function call
        self.image_set._create_des_aoi_table()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_table in all_tables_after, 'Desired AOI table failed to create')

    def test4__create_sat_grid(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table not in all_tables_before,
                        'Desired AOI grid table exists prior to call to _create_download_url: %s' % self.des_aoi_grid_table)

        # function call
        self.image_set._create_sat_grid()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.des_aoi_grid_table in all_tables_after, 'Desired AOI grid table failed to create')

    def test5_create_download_url(self):
        # get list of imagery tables before function call
        # ensure table does not exist yet
        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table not in all_tables_before,
                        'Download url table exists prior to call to _create_download_url: %s' % self.download_url_table)

        # function call
        self.image_set._create_download_url()

        # get list of imagery tables after function call
        # assert table exists
        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]
        self.assertTrue(self.download_url_table in all_tables_after, 'Download url table failed to create')

    def test6_save_url_csv(self):
        # assert file does not exist yet
        self.assertTrue(not os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv file already exists: %s' % self.image_set.download_url_csv)

        # function call
        self.image_set._save_url_csv()

        # assert file now exists
        self.assertTrue(os.path.isfile(self.image_set.download_url_csv),
                        'Download url csv failed to create: %s' % self.image_set.download_url_csv)

    def test7_csv_to_s3(self):
        response_before = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        if 'Contents' in response_before:
            before_keys = [elem['Key'] for elem in response_before['Contents']]
        else:
            before_keys = []

        # function call
        self.image_set._csv_to_s3()

        response_after = s3.list_objects(
            Bucket=config.OUTPUT_BUCKET,
            Prefix=self.image_set.s3_define_folder
        )

        after_keys = [elem['Key'] for elem in response_after['Contents']]
        file_key = self.image_set.s3_define_folder + config.download_url
        print file_key

        if file_key in before_keys:
            before_time = [elem['LastModified'] for elem in response_before['Contents'] if elem['Key'] == file_key][0]
            after_time = [elem['LastModified'] for elem in response_after['Contents'] if elem['Key'] == file_key][0]
            self.assertTrue(after_time > before_time, 'CSV file failed to update in S3.')
        else:
            self.assertTrue(file_key in after_keys, 'CSV file failed to upload to S3.')

    def test7_remove_db_tables(self):

        all_tables_before = [result[0] for result in get_tables(self.image_set.conn)]

        # assert tables exist before remove
        for table in self.tables:
            self.assertTrue(table in all_tables_before, 'Table does not exist: %s' % table)

        # function call
        self.image_set._remove_db_tables()

        all_tables_after = [result[0] for result in get_tables(self.image_set.conn)]

        # assert table no longer exist
        for table in self.tables:
            self.assertTrue(table not in all_tables_after, 'Table failed to delete: %s' % table)

    def test8_close_conn(self):
        # assert connection not yet closed
        self.assertTrue(self.image_set.conn.closed == 0, 'Connection not closed')

        # close connection
        self.image_set.conn.close()

        # assert connection is closed
        self.assertTrue(self.image_set.conn.closed != 0, 'Connection failed to close')


if __name__ == "__main__":
    unittest.main()

# --------------------------------------------------
# Python module for downloading AWS S3 objects
# It has three main methods: download, recursive_download, and list.
# --------------------------------------------------
import os
import sys
import boto3
import botocore
import logging
import re
from multiprocessing.dummy import Pool as ThreadPool
# --------------------------------------------------

logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('nose').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)


class S3Download:
    """
    Class for transferring S3 objects
    """
    NUM_PROC = 1

    def __init__(self, region=None, aws_profile=None):
        """
        set the object properties
        """
        if aws_profile:
            boto3.setup_default_session(profile_name=aws_profile)
        self.conn = self._make_s3_connection(region=region)
        self.src_list = None
        self._bucket = None

    @staticmethod
    def _make_s3_connection(region=None):
        """
        Makes AWS S3 connection
        :param region: AWS region
        :return: connection object
        """
        try:
            conn = boto3.resource('s3', region_name=region)
        except Exception as exp:
            logger.error('Could not make a connection to the S3 resource! Region: {}, Error: {}'.format(region, exp))
            return

        return conn

    def _get_bucket(self, bucket_name):
        """
        Gets a bucket and assigns it to self._bucket if it is None or its name is different
        :param bucket_name: bucket name
        :return: None
        """
        if self._bucket is None or self._bucket.name != bucket_name:
            try:
                self._bucket = self.conn.Bucket(bucket_name)
            except Exception as e:
                logger.error('Could not connect to the S3 bucket! Bucket name: {}, Error: {}'.format(bucket_name, e))

    @staticmethod
    def _s3_path_parser(s3_path):
        """
        Extracts bucket name and the remainder of a S3 path
        :param s3_path: S3 path
        :return: dictionary of bucket name and the remainder of the S3 path
        """
        if type(s3_path) is not str:
            logger.error("The type of the input S3 location is invalid! "
                         "The type is {} (must be str)".format(type(s3_path)))

        if s3_path.startswith('s3://'):
            s3_path = s3_path.replace('s3://', '')
        bucket_name = s3_path.split('/')[0]
        tail = s3_path.replace('{}/'.format(bucket_name), '')

        return dict(bucket_name=bucket_name, object_name=tail)

    def _download(self, inp):
        """
        Downloads a S3 object to a local location
        :param src_key: source key name
        :param dst_path: destination path
        :param overwrite: if True, overwrites on the destination file if exists
        :return: None
        """
        src_key, dst_path, overwrite = inp

        if not overwrite and os.path.exists(dst_path):
            logger.info("{}, already EXISTS - skipped".format(dst_path))
        else:
            try:
                self._bucket.download_file(src_key, dst_path)
                #logger.info('File is downloaded, destination: ''{} , source: s3://{}/{}'.format(dst_path, self._bucket.name, src_key))
            except IOError as ioe:
                logger.error("Failed to download to the destination! Destination: "
                             "{}, source: s3://{}/{}, I/O Error: {}".format(dst_path, self._bucket, src_key, ioe))
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logger.error("The source object does NOT exist! "
                                 "Object: s3://{}/{} , Error: {}".format(self._bucket, src_key, e))
                else:
                    logger.error("Failed to download! "
                                 "Object: s3://{}/{} , Error: {}".format(self._bucket, src_key, e))
            except Exception as e:
                logger.error("Failed to download! Object: s3://{}/{} , Error: {}".format(self._bucket, src_key, e))

    def download(self, s3_path, dst_dir, overwrite=False):
        """
        Entry method to download a S3 object to a local location
        :param s3_path: S3 path
        :param dst_dir: destination directory
        :param overwrite: if True, overwrites on the destination file if exists
        :return: None
        """
        if type(s3_path) != str or type(dst_dir) != str:
            logger.error("Invalid input types (must be str)! "
                         "types of s3_path, dst_dir: {}, {}".format(type(s3_path), type(dst_dir)))
            return

        if self.conn is None:
            return

        s3_path = self._s3_path_parser(s3_path)
        if s3_path is None:
            return

        self._get_bucket(s3_path['bucket_name'])
        if self._bucket is None:
            return

        dst_dir = self._standardize_dir_path(dst_dir)
        dst_path = os.path.join(os.path.expanduser(os.path.normpath(dst_dir)), s3_path['object_name'].split('/')[-1])
        self._download((s3_path['object_name'], dst_path, overwrite))

    def _list_objects(self, object_prefix):
        """
        Obtains the information of S3 keys under a prefix
        :param object_prefix: the prefix of the keys
        :return: list of dictionaries of bucket_nam, 'object_name, and size
        """
        try:
            keys = list(self._bucket.objects.filter(Prefix=object_prefix))
        except Exception as e:
            logger.error("Failed to list the objects under "
                         "s3://{}/{} , Error: {}".format(self._bucket, object_prefix, e))
            return None

        keys = [{'bucket_name': k.bucket_name, 'object_name': k.key, 'size': k.size} for k in keys]

        return keys

    @staticmethod
    def _filter_object_names(keys, include_re_pattern=None, exclude_re_pattern=None):
        """
        Filters the list of S3 keys based on regular expression patterns
        :param keys: list of blob objects
        :param include_re_pattern: regular expression pattern to search and include the object names
        :param exclude_re_pattern: regular expression pattern to search and exclude the object names
        :return: list of filtered keys
        """

        if include_re_pattern:
            re_pattern = re.compile(include_re_pattern)
            keys = [k for k in keys if re_pattern.search(k['object_name']) is not None]

        if exclude_re_pattern:
            re_pattern = re.compile(exclude_re_pattern)
            keys = [k for k in keys if re_pattern.search(k['object_name']) is None]

        return keys

    def list(self, s3_path, include_re_pattern=None, exclude_re_pattern=None):
        """
        Entry method to obtain the information of S3 keys under a prefix
        :param s3_path: S3 path
        :param include_re_pattern: regular expression pattern to search and include the object names
        :param exclude_re_pattern: regular expression pattern to search and exclude the object names
        :return:
        """
        if type(s3_path) != str or (include_re_pattern is not None and type(include_re_pattern) != str) or \
                (exclude_re_pattern is not None and type(exclude_re_pattern) != str):
            logger.error("Invalid input types (must be str)! types of s3_path, include_re_pattern, exclude_re_pattern: "
                         "{}, {}, {}".format(type(s3_path), type(include_re_pattern), type(exclude_re_pattern)))
            return

        if self.conn is None:
            return

        s3_path = self._s3_path_parser(s3_path)
        if s3_path is None:
            return

        self._get_bucket(s3_path['bucket_name'])
        if self._bucket is None:
            return

        keys = self._list_objects(s3_path['object_name'])
        if keys is None:
            return

        if include_re_pattern or exclude_re_pattern:
            keys = self._filter_object_names(keys, include_re_pattern, exclude_re_pattern)

        return keys

    @staticmethod
    def _standardize_dir_path(dir_path):
        """
        If the directory does not end with '/' or '\\' based on the OS, adds this character at the end
        :param dir_path: local directory path
        :return: local directory path
        """
        is_windows = sys.platform.lower().startswith('win')
        if is_windows:
            dir_path = dir_path if dir_path.endswith('\\') else "{}\\".format(dir_path)
        else:
            dir_path = dir_path if dir_path.endswith('/') else "{}/".format(dir_path)

        return dir_path

    def _make_local_path(self, object_name, prefix_to_replace, base_dst_dir):
        """
        Maps the destination file path based on the input object name, and creates sub-directories if needed
        :param object_name: object name of interest
        :param prefix_to_replace: prefix of the object name being replaced with the local destination directory path
        :param base_dst_dir: base local destination directory
        :return: local destination file path corresponding to the input object name and prefix_to_replace
        """
        filename = object_name.split('/')[-1]

        dst_path_tail = object_name.replace(prefix_to_replace, '')
        if sys.platform.lower().startswith('win'):
            # For Windows, replaces '/' with '\' in the blob name
            dst_path_tail = dst_path_tail.replace('/', '\\')
            if dst_path_tail.startswith('\\'):
                # Removes the last '\\' if exists
                dst_path_tail = object_name[2:]
        elif dst_path_tail.startswith('/'):
            # Removes the last '/' if exists
            dst_path_tail = dst_path_tail[1:]

        # Normalizing and standardizing the local path
        base_dst_dir = self._standardize_dir_path(base_dst_dir)
        base_dst_dir = os.path.expanduser(os.path.normpath(base_dst_dir))

        # Making the destination directory string
        dst_dir = os.path.join(base_dst_dir, os.path.dirname(dst_path_tail))

        # Creates sub-directories based on the destination directory path
        if os.path.dirname(dst_dir) != '' and not os.path.exists(dst_dir):
            try:
                os.makedirs(dst_dir)
            except IOError as ioe:
                logger.error('I/O Error: could NOT build the directory: {} , error: {}'.format(dst_dir, ioe))
                return None
            except Exception as exp:
                logger.error('Could NOT build the directory: {} , error: {}'.format(dst_dir, exp))
                return None

        dst_path = os.path.join(dst_dir, filename)

        return dst_path

    def recursive_download(self, s3_path, base_dst_dir, folder_map=True, overwrite=False,
                           prefix_to_replace=None, include_re_pattern=None, exclude_re_pattern=None):
        """
        Entry function to recursively download the blobs under an AWS prefix
        :param s3_path: S3 path (e.g. s3://bucket-name/some/prefix or bucket-name/some/prefix)
        :param base_dst_dir: base local destination directory
        :param folder_map: whether map the folders under the prefix into the base destination directory (True), or dump
                           all the files to the base destination directory regardless of the folder structure under the
                           S3 path prefix (if False, no folder will be created under the base destination directory)
        :param overwrite: whether overwrite if the destination file already exists
        :param prefix_to_replace: object name prefix (excluding the bucket name) to be replaced when making
                                  subdirectories. If it is None (default value), the prefix_to_replace will be set to
                                  the object prefix of s3_path.
        :param include_re_pattern: regular expression pattern to search and include the object names
        :param exclude_re_pattern: regular expression pattern to search and exclude the object names
        :return: None
        """
        if type(base_dst_dir) != str or (prefix_to_replace is not None and type(prefix_to_replace) != str):
            logger.error("Invalid input types! types of base_dst_dir, prefix_to_replace: "
                         "{}, {}".format(type(base_dst_dir), type(prefix_to_replace)))

        keys = self.list(s3_path, include_re_pattern=include_re_pattern, exclude_re_pattern=exclude_re_pattern)

        if keys is None or len(keys) == 0:
            logger.info('No object is found under the input AWS S3 path to recursively download! '
                        'Input path: {}'.format(s3_path))
            return

        logger.info('Number of found objects under the input AWS S3 path: '
                    '{} , Input path: {}'.format(len(keys), s3_path))

        s3_parsed = self._s3_path_parser(s3_path)
        key_prefix = prefix_to_replace if prefix_to_replace else s3_parsed['object_name']

        download_args = []
        for k in keys:
            if folder_map:
                dst_path = self._make_local_path(k['object_name'], key_prefix, base_dst_dir)
            else:
                dst_path = os.path.join(os.path.expanduser(os.path.normpath(base_dst_dir)), k['object_name'].split('/')[-1])

            if dst_path:
                # self._download(k['object_name'], dst_path, overwrite=overwrite)
                download_args.append((k['object_name'], dst_path, overwrite))

        pool = ThreadPool(S3Download.NUM_PROC)
        pool.map(self._download, download_args)

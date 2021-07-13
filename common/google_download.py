# --------------------------------------------------
# Python module for downloading Google Storage objects
# It has three main methods: download, recursive_download, and list.
# --------------------------------------------------
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.exceptions import NotFound
import os
import sys
from os import path
import logging
import re
from multiprocessing.dummy import Pool as ThreadPool
# --------------------------------------------------

logger = logging.getLogger(__name__)


class GoogleStorageDownload():
    """Class for downloading and listing Google Storage objects/blobs"""
    NUM_PROC = 1

    def __init__(self, anonymous=True):
        """
        Sets the initial object variables
        :param anonymous: Whether creating an anonymous client or using default credentials (needs to set the configuration files)
        """
        self.client = None
        self._get_client(anonymous=anonymous)

        self.bucket = None

    @staticmethod
    def _parse_gs_path(gs_path):
        """
        Parses the input Google Storage path
        :param gs_path: Google Storage path
        :return: tuple of bucket name and blob name
        """
        if type(gs_path) is not str:
            logger.error('The type of the input Google Storage is NOT a string! Type: {}'.format(type(gs_path)))
            return None, None

        if gs_path.startswith('gs://') is not True:
            logger.error('The input Google Storage does NOT start with "gs://"! Input path: {}'.format(gs_path))
            return None, None

        gs_path = gs_path.replace('gs://', '')
        bucket_name = gs_path.split('/')[0]

        try:
            blob_name = gs_path.replace('{}/'.format(bucket_name), '')
        except:
            blob_name = ''

        return bucket_name, blob_name

    def _get_client(self, anonymous=True):
        """
        Creates a Google Cloud client and assigns it to self.client
        :param anonymous: Whether creating an anonymous client or using default credentials (needs to set the configuration files)
        :return: None
        """
        if anonymous:
            try:
                self.client = storage.client.Client(project='<none>', credentials=AnonymousCredentials())
            except Exception as exp:
                logger.error('Could not make an anonymous google-cloud client! The error message: {}'.format(exp.message))
        else:
            try:
                # Requires setting the default configurations
                self.client = storage.client.Client()
            except DefaultCredentialsError as exp:
                logger.error('Could not make a google-cloud client using default settings! The error message: {}'.format(exp.message))
            except Exception as exp:
                logger.error('Could not make a google-cloud client! The error message: {}'.format(exp.message))

    def _get_bucket(self, bucket_name):
        """
        Creates a bucket object and assigns it to self.client
        :param bucket_name: targeted bucket name
        :return: None
        """
        if self.client is None:
            return

        try:
            # Avoid making a new bucket object if it is already made for the same bucket name
            if self.bucket is not None and self.bucket.name != bucket_name:
                self.bucket = None
            if self.bucket is None:
                self.bucket = storage.bucket.Bucket(self.client, name=bucket_name)
        except NotFound as exp:
            logger.error('Could not get the bucket name of {}! The error message: {}'.format(bucket_name, exp.message))

    @staticmethod
    def _normalize_dir_path(dir_path):
        """
        If the directory does not end with '/' or '\' based on the OS, adds this character at the end
        :param dir_path: local directory path
        :return: local directory path
        """
        is_windows = sys.platform.lower().startswith('win')
        if is_windows:
            dir_path = dir_path if dir_path.endswith('\\') else "{}\\".format(dir_path)
        else:
            dir_path = dir_path if dir_path.endswith('/') else "{}/".format(dir_path)

        return dir_path

    def _download(self, inp):
        """
        Executes downloading a blob
        :param blob_name: blob name
        :param dst_file_path: local destination file path
        :param overwrite: whether overwrite if the destination file already exists
        :return: None
        """
        blob_name, dst_file_path, overwrite = inp

        if not overwrite and os.path.exists(dst_file_path):
            logger.info('{} already EXISTS - skipped downloading'.format(dst_file_path))
            return

        # The generic Google Cloud package does not create a temporary file for downloading, so, if the download is incomplete,
        # there will be a corrupted file with the targeted file name. So, the following line is to change the file name to a
        # temporary name, which later will be renamed to the targeted file name
        temporary_path = '{}.000'.format(dst_file_path)

        try:
            self.bucket.blob(blob_name).download_to_filename(temporary_path)
            os.rename(temporary_path, dst_file_path)
            logger.info('File is downloaded, destination:{}, source: gs://{}/{} has been downloaded'.format(dst_file_path, self.bucket.name, blob_name))
        except IOError as ioe:
            logger.error('Could NOT download the Google Cloud blob! Source: gs://{}/{}, destination: {}. IOError: {}'.format(self.bucket.name, blob_name, dst_file_path, ioe))
        except NotFound as nfe:
            logger.error('Could NOT find the source file: gs://{}/{}'.format(self.bucket.name, blob_name))
        except Exception as exp:
            logger.error('Could NOT download the Google Cloud blob! Source: gs://{}/{}, destination: {}. Error: {}'.format(self.bucket.name, blob_name, dst_file_path, exp))

        # Collect the garbage
        if os.path.exists(temporary_path):
            try:
                os.remove(temporary_path)
            except IOError as ioe:
                logger.error('I/O Error: could NOT delete the downloaded temporary file! Temporary file: {}. IOError: {}'.format(temporary_path, ioe))
            except Exception as exp:
                logger.error('Could NOT delete the downloaded temporary file! Temporary file: {}. Error: {}'.format(temporary_path, exp))

    def download(self, gs_path, dst_dir, overwrite=False):
        """
        Entry function to download a blob
        :param gs_path: Google Storage path (e.g. gs://bucket-name/blob/object)
        :param dst_dir: local destination directory
        :param overwrite: whether overwrite if the destination file already exists
        :return: None
        """
        bucket_name, blob_name = self._parse_gs_path(gs_path)
        if blob_name in (None, '') or bucket_name is None:
            logger.error('The input Google storage path does not have valid bucket and/or blob names! Input path: {}'.format(gs_path))
            return

        self._get_bucket(bucket_name)
        if self.bucket is None:
            return

        dst_dir = self._normalize_dir_path(dst_dir)
        filename = blob_name.split('/')[-1]
        dst_file_path = path.join(self._normalize_dir_path(dst_dir), filename)

        self._download((blob_name, dst_file_path, overwrite))

    def _list(self, gs_path):
        """
        Executes listing the blobs under a Google Storage prefix
        :param gs_path: Google Storage path
        :return: list of blob objects
        """
        bucket_name, blob_prefix = self._parse_gs_path(gs_path)
        if blob_prefix in (None, '') or bucket_name is None:
            logger.error('The input Google storage path does not have valid bucket name and/or blob prefix! Input path: {}'.format(gs_path))
            return

        self._get_bucket(bucket_name)
        if self.bucket is None:
            return

        try:
            blobs = list(self.bucket.list_blobs(prefix=blob_prefix))
        except NotFound as nfe:
            logger.error('Could NOT find the blob prefix: gs://{}/{}'.format(self.bucket.name, blob_prefix))
            return None
        except Exception as exp:
            logger.error('Could NOT list the objects under gs://{}/{}'.format(self.bucket.name, blob_prefix))
            return None

        return blobs

    @staticmethod
    def _filter_blob_names(blobs, include_re_pattern=None, exclude_re_pattern=None):
        """
        Filters the the list of blobs based on the blob_name_pattern
        :param blobs: list of blob objects
        :param include_re_pattern: regular expression pattern to search and include the blob names
        :param exclude_re_pattern: regular expression pattern to search and exclude the blob names
        :return: list of filtered blob objects
        """
        if include_re_pattern:
            re_pattern = re.compile(include_re_pattern)
            blobs = [b for b in blobs if re_pattern.search(b.name) is not None]

        if exclude_re_pattern:
            re_pattern = re.compile(exclude_re_pattern)
            blobs = [b for b in blobs if re_pattern.search(b.name) is None]

        return blobs

    def list(self, gs_path, include_re_pattern=None, exclude_re_pattern=None):
        """
        Entry function to list the blobs under a Google Storage prefix
        :param gs_path: Google Storage path (e.g. gs://bucket-name/blob/prefix/)
        :param include_re_pattern: regular expression pattern to search and include the object names
        :param exclude_re_pattern: regular expression pattern to search and exclude the object names
        :return: list of dictionaries with the keys of 'bucket_name', 'blob_name', and 'blob_size'
        """
        if type(gs_path) != str or (include_re_pattern is not None and type(include_re_pattern) != str) or \
                (exclude_re_pattern is not None and type(exclude_re_pattern) != str):
            logger.error("Invalid input types (must be str)! types of gs_path, include_re_pattern, exclude_re_pattern: "
                         "{}, {}, {}".format(type(gs_path), type(include_re_pattern), type(exclude_re_pattern)))
            return

        blobs = self._list(gs_path)
        if blobs is None:
            return

        if include_re_pattern or exclude_re_pattern:
            blobs = self._filter_blob_names(blobs, include_re_pattern, exclude_re_pattern)

        try:
            blobs = [{'bucket_name':x.bucket, 'blob_name': x.name, 'blob_size': x.size} for x in blobs]
        except Exception as exp:
            logger.error('Could not make a list of object information under {} , Error: {}'.format(gs_path, exp))
            return

        return blobs

    def _make_local_path(self, blob_name, prefix_to_replace, base_dst_dir):
        """
        Maps the destination file path based on the input blob name, and creates sub-directories if needed
        :param blob_name: blob name of interest
        :param prefix_to_replace: prefix of the blob name to be replaced with the local destination directory path
        :param base_dst_dir: base local destination directory
        :return: local destination file path corresponding to the input blob name and the prefix-to-replace
        """
        filename = blob_name.split('/')[-1]

        dst_path_tail = blob_name.replace(prefix_to_replace, '')
        if sys.platform.lower().startswith('win'):
            # For Windows, replaces '/' with '\' in the blob name
            dst_path_tail = dst_path_tail.replace('/', '\\')
            if dst_path_tail.startswith('\\'):
                # Removes the last '\\' if exists
                dst_path_tail = blob_name[2:]
        elif dst_path_tail.startswith('/'):
            # Removes the last '/' if exists
            dst_path_tail = dst_path_tail[1:]

        # Normalizing and standardizing the local path
        base_dst_dir = self._normalize_dir_path(base_dst_dir)
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

    def recursive_download(self, gs_path, base_dst_dir, folder_map=True, overwrite=False, prefix_to_replace=None,
                           include_re_pattern=None, exclude_re_pattern=None):
        """
        Entry function to recursively download the blobs under a Google Storage prefix
        :param gs_path: Google Storage path (e.g. gs://bucket-name/blob/prefix/)
        :param base_dst_dir: base local destination directory
        :param folder_map: whether map the folders under the prefix into the base destination directory (True), or dump all the files
                           to the base destination directory regardless of the folder structure under the Google Storage
                           prefix (if False, no folder will be created under the base destination directory)
        :param overwrite: whether overwrite if the destination file already exists
        :param prefix_to_replace: blob name prefix (excluding the bucket name) to be replaced when making subdirectories.
                                  If it is None (default value), the prefix_to_replace will be set to the blob prefix of gs_path.
        :param include_re_pattern: regular expression pattern to search and include the object names
        :param exclude_re_pattern: regular expression pattern to search and exclude the object names
        :return: None
        """
        blobs = self.list(gs_path, include_re_pattern=include_re_pattern, exclude_re_pattern=exclude_re_pattern)

        if blobs is None or len(blobs) == 0:
            logger.info('No blob is found under the input Google storage path to recursively download! Input path: {}'.format(gs_path))
            return

        logger.info('Number of found blobs under the input Google storage path: {} , Input path: {}'.format(len(blobs),gs_path))

        bucket_name, blob_prefix = self._parse_gs_path(gs_path)
        blob_prefix = prefix_to_replace if prefix_to_replace else blob_prefix

        download_args = []
        for blob in blobs:
            if folder_map:
                dst_path = self._make_local_path(blob['blob_name'], blob_prefix, base_dst_dir)
            else:
                dst_path = path.join(os.path.expanduser(os.path.normpath(base_dst_dir)), blob['blob_name'].split('/')[-1])

            if dst_path:
                # self._download(blob['blob_name'], dst_path, overwrite=overwrite)
                download_args.append((blob['blob_name'], dst_path, overwrite))

        pool = ThreadPool(GoogleStorageDownload.NUM_PROC)
        pool.map(self._download, download_args)
import os
import logging
import ConfigParser
import hvac
from datetime import datetime
import uuid
from orthoimagery_pipeline.common.exceptions import SecretNotFoundException

# resolve config file paths relative to current file
config_dir = os.path.join(os.path.dirname(__file__), 'conf')
common_conf = os.path.join(config_dir, 'common.conf')
define_config = os.path.join(config_dir, 'define_image_set.conf')
download_config = os.path.join(config_dir, 'download_content.conf')
evi_config = os.path.join(config_dir, 'evi_process.conf')
publish_config = os.path.join(config_dir, 'publish_granule.conf')
validate_config = os.path.join(config_dir, 'validate_content.conf')

logger = logging.getLogger(__name__)

# hardcode these environmental variables until vault is authenticated
APP_ENV = os.getenv("APP_ENV")
HABITAT = os.getenv("APP_HABITAT")
# assert the env vars aren't empty
assert APP_ENV in ('prod', 'np')
if HABITAT :
    assert APP_ENV == 'np'

# specify product_habitat, which contains a desired set of output from the pipeline based on AOI and time frame
PRODUCT_HABITAT = os.getenv("PRODUCT_HABITAT")

DB_KEYS = ('DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS')
GS_KEYS = ('GS_HOST', 'GS_USERNAME', 'GS_ROLES')

app_config = ConfigParser.ConfigParser()
app_config.read(common_conf)

# S3 Bucket and folder configuration
OUTPUT_BUCKET = app_config.get('prod', 'OUTPUT_BUCKET') if APP_ENV == 'prod' else app_config.get('np', 'OUTPUT_BUCKET')
DEFINE_PREFIX = '{}/{}'.format(PRODUCT_HABITAT, app_config.get('all', 'DEFINE_PREFIX')) if not HABITAT \
    else '{}/{}/{}'.format(HABITAT, PRODUCT_HABITAT, app_config.get('all', 'DEFINE_PREFIX'))
VALIDATE_PREFIX = '{}/{}'.format(PRODUCT_HABITAT, app_config.get('all', 'VALIDATE_PREFIX')) if not HABITAT \
    else '{}/{}/{}'.format(HABITAT, PRODUCT_HABITAT, app_config.get('all', 'VALIDATE_PREFIX'))
DOWNLOAD_PREFIX = '{}/{}'.format(PRODUCT_HABITAT, app_config.get('all', 'DOWNLOAD_PREFIX')) if not HABITAT \
    else '{}/{}/{}'.format(HABITAT, PRODUCT_HABITAT, app_config.get('all', 'DOWNLOAD_PREFIX'))
EVI_PREFIX = '{}/{}'.format(PRODUCT_HABITAT, app_config.get('all', 'EVI_PREFIX')) if not HABITAT \
    else '{}/{}/{}'.format(HABITAT, PRODUCT_HABITAT, app_config.get('all', 'EVI_PREFIX'))

L7_DEFINE_FOLDER = DEFINE_PREFIX + 'l7/'
L8_DEFINE_FOLDER = DEFINE_PREFIX + 'l8/'
SENTINEL_DEFINE_FOLDER = DEFINE_PREFIX + 'sentinel/'

L7_VALIDATE_FOLDER = VALIDATE_PREFIX + 'l7/'
L8_VALIDATE_FOLDER = VALIDATE_PREFIX + 'l8/'
SENTINEL_VALIDATE_FOLDER = VALIDATE_PREFIX + 'sentinel/'

L7_DOWNLOAD_FOLDER = DOWNLOAD_PREFIX + 'l7/'
L8_DOWNLOAD_FOLDER = DOWNLOAD_PREFIX + 'l8/'
SENTINEL_DOWNLOAD_FOLDER = DOWNLOAD_PREFIX + 'sentinel/'

L7_EVI_FOLDER = OUTPUT_BUCKET + '/' + EVI_PREFIX + 'l7/'
L8_EVI_FOLDER = OUTPUT_BUCKET + '/' + EVI_PREFIX + 'l8/'
SENTINEL_EVI_FOLDER = OUTPUT_BUCKET + '/' + EVI_PREFIX + 'sentinel/'

today = datetime.utcnow()
image_acquisitiontime = today
YEARMO_FMT = '%Y%m'
DATE_FMT = '%Y%m%d'
imagemonth = image_acquisitiontime.strftime(YEARMO_FMT)
imageday = image_acquisitiontime.strftime(DATE_FMT)

# File and Folder configuration in processing instance
download_url = 'image_list_' + imageday + '.csv'
download_url_inval = 'inval_image_list_' + imageday + '.csv'

# to restrict file numbers in a single folder on EFS less than 8000
# granules are saved to folderes which match the processing year and month (format: YYYYMM)
DATA_DIR_PREFIX = app_config.get(APP_ENV, 'DATA_DIR_PREFIX') + PRODUCT_HABITAT if not HABITAT \
    else app_config.get(APP_ENV, 'DATA_DIR_PREFIX').format(HABITAT) + PRODUCT_HABITAT

PUB_GRANULE_FOLDER = app_config.get(APP_ENV, 'PUB_GRANULE_FOLDER') + imagemonth if not HABITAT \
    else app_config.get(APP_ENV, 'PUB_GRANULE_FOLDER').format(HABITAT) + imagemonth

L7_DATA_FOLDER = DATA_DIR_PREFIX + '/l7/'
L8_DATA_FOLDER = DATA_DIR_PREFIX + '/l8/'
SENTINEL_DATA_FOLDER = DATA_DIR_PREFIX + '/sentinel/'

# cloud masking and outputs from EVI calculation
toaImage = app_config.get('all', 'toaImage')
cloud = app_config.get('all', 'cloud')
toaImage_s2 = app_config.get('all', 'toaImage_s2')


class VaultClient(object):
    '''
    Singleton wrapper for vault clients
    '''

    _client = None

    @classmethod
    def instance(cls):
        if not cls._client:
            vault_url = app_config.get('vault', 'VAULT_URL')
            role_id = os.getenv('VAULT_ROLE_ID')
            secret_id = os.getenv('VAULT_SECRET_ID')

            if role_id is None:
                raise SecretNotFoundException('Missing required environment variable for Vault credential access: '
                                              'VAULT_ROLE_ID')
            if secret_id is None:
                raise SecretNotFoundException('Missing required environment variable for Vault credential access: '
                                              'VAULT_SECRET_ID')

            cls._client = hvac.Client(url=vault_url)
            client_auth = cls._client.auth_approle(role_id, secret_id)
            cls._client.token = client_auth['auth']['client_token']
        return cls._client


def get_secrets(refresh=False):
    '''
    Function to provide a single secrets dictionary across entire module
    '''

    def resolve_secret(key):
        '''
        Private helper function to resolve secret from either environment or vault
        '''
        if os.getenv(key):
            logger.info('Found secret key in environment: ' + key)
            return os.getenv(key)
        logger.warning('Key ' + ' not found in environment. Checking for Vault secret.')

        client = VaultClient.instance()
        secret_key = app_config.get(APP_ENV, key)
        try:
            return client.read(secret_key)['data']['secret']
        except KeyError:
            raise SecretNotFoundException('No secret available for key: %' % key)

    if refresh:
        get_secrets._secrets = {}
    if get_secrets._secrets:
        return get_secrets._secrets

    for key in DB_KEYS + GS_KEYS:
        get_secrets._secrets[key] = resolve_secret(key)

    return get_secrets._secrets
get_secrets._secrets = {}  # private dictionarty of secrets - should be accessed only through get_secrets() call


def get_conn_string():
    # validate all necessary env vars are set for db connection
    secrets = get_secrets()
    conn_params = {key: secrets[key] for key in DB_KEYS}

    conn_str = "host='{DB_HOST}' port={DB_PORT} dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASS}'".format(**conn_params)

    return conn_str

def generate_uuid():
    return str(uuid.uuid1())
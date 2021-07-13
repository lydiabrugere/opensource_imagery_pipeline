import os
import unittest
import orthoimagery_pipeline.common.config as config
import re

file_dir = os.path.abspath(os.path.dirname(__file__))
fixtures_dir = os.path.join(file_dir, 'fixtures')
bad_define_file = os.path.join(fixtures_dir, 'image_list_bad.csv')
good_define_file = os.path.join(fixtures_dir, 'image_list_good.csv')


app_config = config.app_config
test_env = config.APP_ENV
uuid_regex = re.compile('[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}')


class TestVaultClient(unittest.TestCase):

    vars = {}

    @classmethod
    def setUpClass(cls):
        '''
        Capture environment variables in local cache and unset them if set
        '''
        for key in config.DB_KEYS:
            if os.getenv(key):
                cls.vars[key] = os.getenv(key)
                os.unsetenv(key)
        for key in config.GS_KEYS:
            if os.getenv(key):
                cls.vars[key] = os.getenv(key)
                os.unsetenv(key)

    def test_vault_client_singleton(self):
        vc1 = config.VaultClient.instance()
        vc2 = config.VaultClient.instance()
        self.assertTrue(vc1 is vc2)

    def test_vault_client_db_creds(self):
        vc = config.VaultClient.instance()
        for key in config.DB_KEYS:
            secret = vc.read(app_config.get(test_env, key))['data']['secret']
            self.assertTrue(secret is not None)

    def test_vault_client_gs_creds(self):
        '''
        Restore any environment
        '''
        vc = config.VaultClient.instance()
        for key in config.GS_KEYS:
            secret = vc.read(app_config.get(test_env, key))['data']['secret']
            self.assertTrue(secret is not None)

    @classmethod
    def tearDownClass(cls):
        for key, value in cls.vars.iteritems():
            os.environ[key] = value


class TestGetUUID(unittest.TestCase):

    def test_uuid_is_str(self):

        uuid = config.generate_uuid()

        self.assertTrue(type(uuid) is str)

    def test_uuid_matches(self):

        uuid = config.generate_uuid()

        self.assertIsNotNone(uuid_regex.search(uuid))


if __name__ == "__main__":
    unittest.main()

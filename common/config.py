from configparser import ConfigParser
import os

from .utils import get_logger

class BentoConfig:
    def __init__(self, config_file):
        self.log = get_logger('Bento Config')
        self.PSWD_ENV = 'NEO_PASSWORD'

        config = ConfigParser()
        if config_file and os.path.isfile(config_file):
            config.read(config_file)
        else:
            msg = f'Can NOT open configuration file "{config_file}"!'
            self.log.error(msg)
            raise Exception(msg)

        self.LOG_LEVEL = os.environ.get('DL_LOG_LEVEL', config.get('log', 'log_level'))
        self.QUEUE_LONG_PULL_TIME = int(config.get('sqs', 'long_pull_time'))
        self.VISIBILITY_TIMEOUT = int(config.get('sqs', 'visibility_timeout'))

        self.TEMP_FOLDER = config.get('main', 'temp_folder')
        self.BACKUP_FOLDER = config.get('main', 'backup_folder')
        self.INDEXD_GUID_PREFIX = config.get('indexd', 'GUID_prefix')
        self.INDEXD_MANIFEST_EXT = config.get('indexd', 'ext')

        self.REL_PROP_DELIMITER = config.get('data', 'rel_prop_delimiter')

        if not self.INDEXD_MANIFEST_EXT.startswith('.'):
            self.INDEXD_MANIFEST_EXT = '.' + self.INDEXD_MANIFEST_EXT

        self.SLACK_URL = config.get('slack', 'url')

        self._create_backup_folder()

    def _create_backup_folder(self):
        os.makedirs(self.BACKUP_FOLDER, exist_ok=True)
        if not os.path.isdir(self.BACKUP_FOLDER):
            msg = f'{self.BACKUP_FOLDER} is not a folder!'
            self.log.error(msg)
            raise Exception(msg)




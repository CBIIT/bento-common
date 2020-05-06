from configparser import ConfigParser
import os
import yaml

from .utils import get_logger

class BentoConfig:
    def __init__(self, config_file):
        self.log = get_logger('Bento Config')
        self.PSWD_ENV = 'NEO_PASSWORD'

        if config_file and os.path.isfile(config_file):
            with open(config_file) as c_file:
                config = yaml.safe_load(c_file)['Config']

                if 'sqs' in config:
                    sqs = config['sqs']
                    self.QUEUE_LONG_PULL_TIME = sqs.get('long_pull_time')
                    self.VISIBILITY_TIMEOUT = sqs.get('visibility_timeout')

                self.TEMP_FOLDER = config.get('temp_folder')
                self.BACKUP_FOLDER = config.get('backup_folder')
                if 'indexd' in config:
                    indexd = config['indexd']
                    self.INDEXD_GUID_PREFIX = indexd.get('GUID_prefix')
                    self.INDEXD_MANIFEST_EXT = indexd.get('ext')
                    if self.INDEXD_MANIFEST_EXT and not self.INDEXD_MANIFEST_EXT.startswith('.'):
                        self.INDEXD_MANIFEST_EXT = '.' + self.INDEXD_MANIFEST_EXT

                self.REL_PROP_DELIMITER = config.get('rel_prop_delimiter')

                self.SLACK_URL = config.get('url')

                self._create_backup_folder()
        else:
            msg = f'Can NOT open configuration file "{config_file}"!'
            self.log.error(msg)
            raise Exception(msg)


    def _create_backup_folder(self):
        os.makedirs(self.BACKUP_FOLDER, exist_ok=True)
        if not os.path.isdir(self.BACKUP_FOLDER):
            msg = f'{self.BACKUP_FOLDER} is not a folder!'
            self.log.error(msg)
            raise Exception(msg)




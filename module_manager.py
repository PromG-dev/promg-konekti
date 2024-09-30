from promg import DatabaseConnection

from promg import Performance
from promg.modules.db_management import DBManagement
from promg import Configuration

from promg_ocel.module.extra_constraint_manager import ConstraintsManager
from promg_ocel.module.oced_import import OcedImport


class ModuleManager:
    def __init__(self, config):
        if config is None:
            config = Configuration.init_conf_with_config_file()
        self._config = config
        self._db_connection = DatabaseConnection.set_up_connection(config=config)
        self._performance = Performance.set_up_performance(config=config)


        self._db_manager = None
        self._constraints_manager = None
        self._oced_import_module = None

    def get_config(self):
        return self._config

    def get_is_preprocessed_files_used(self):
        return self._config.use_preprocessed_files

    def get_db_connection(self):
        return self._db_connection

    def get_performance(self):
        return self._performance

    def get_db_manager(self):
        if self._db_manager is None:
            self._db_manager = DBManagement(db_connection=self._db_connection)
        return self._db_manager

    def get_constraints_manager(self):
        if self._constraints_manager is None:
            self._constraints_manager = ConstraintsManager(db_connection=self._db_connection)
        return self._constraints_manager

    def get_oced_import_module(self):
        if self._oced_import_module is None:
            self._oced_import_module = OcedImport(db_connection=self._db_connection)
        return self._oced_import_module

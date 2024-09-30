import os
from colorama import Fore
from module_manager import ModuleManager


class MethodManager:
    def __init__(self, config=None, file=None):
        self.modules = ModuleManager(config=config)
        self.file = file

    def clear_database(self):
        db_manager = self.modules.get_db_manager()
        print(Fore.RED + 'Clearing the database.' + Fore.RESET)
        db_manager.clear_db(replace=True)
        db_manager.set_constraints()
        self.set_extra_constraints()

    def set_extra_constraints(self):
        constraints_manager = self.modules.get_constraints_manager()
        constraints_manager.set_event_id_constraint()

    def load_and_import_json(self):
        oi = self.modules.get_oced_import_module()
        oi.read_json_ocel(file=self.file)
        oi.import_objects()
        oi.import_events()
        oi.import_relationships()

    def finish_and_save(self):
        performance = self.modules.get_performance()
        performance.finish_and_save()

    def print_statistics(self):
        db_manager = self.modules.get_db_manager()
        db_manager.print_statistics()

    def close_connection(self):
        db_connection = self.modules.get_db_connection()
        db_connection.close_connection()

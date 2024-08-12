import os
from colorama import Fore
from module_manager import ModuleManager


class MethodManager:
    def __init__(self, config=None, zip_file=None):
        self.modules = ModuleManager(config=config)
        self.zip_file = zip_file

    def clear_database(self):
        db_manager = self.modules.get_db_manager()
        print(Fore.RED + 'Clearing the database.' + Fore.RESET)
        db_manager.clear_db(replace=True)
        db_manager.set_constraints()

    def load_and_import_json(self):
        oi = self.modules.get_oced_import_module()
        oi.readJsonOcel(dataset=self.zip_file)
        oi.import_objects()
        oi.import_events()

        connections = [
            {
                "column_id": "payment_id",
                "labels": ["Payment"],
                "keys": ["bank_account", "created_date_time", "scheduled_bank_run", "created_by"]
            },
            {

            }
        ]

        oi.connect_events_to_objects(column_id="payment_id", labels=["Payment"],
                                     keys=["bank_account", "created_date_time", "scheduled_bank_run", "created_by"])

    def finish_and_save(self):
        performance = self.modules.get_performance()
        performance.finish_and_save()

    def print_statistics(self):
        db_manager = self.modules.get_db_manager()
        db_manager.print_statistics()

    def close_connection(self):
        db_connection = self.modules.get_db_connection()
        db_connection.close_connection()

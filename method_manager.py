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
        # oi.import_objects()
        oi.import_events()

        connections = [
            {
                "column_ids": ["payment_id"],
                "labels": ["Payment"],
                "keys": ["created_date_time", "bank_account"]
            },
            {
                "column_ids": ["invoice_id", "line_id"],
                "labels": ["InvoiceLine"],
                "keys": ["line_id"]
            },
            {
                "column_ids": ["invoice_id"],
                "labels": ["Invoice"],
                "keys": ["created_date_time", "scheduled_bank_run"]
            },
            {
                "column_ids": ["order_id", "business_unit"],
                "labels": ["OrderLine"],
                "keys": ["created_date_time", "business_unit"]
            },
            {
                "column_ids": ["order_id"],
                "labels": ["Order"],
                "keys": ["created_date_time", "supplier"]
            },
            {
                "column_ids": ["goods_receipt_id", "line_id"],
                "labels": ["Goodsreceiptsline"],
                "keys": ["line_id"]
            },
            {
                "column_ids": ["goods_receipt_id"],
                "labels": ["Goodsreceipts"],
                "keys": ["created_date_time"]
            },
            {
                "column_ids": ["created_by"],
                "labels": ["Resource"]
            }
        ]

        all_keys = set()
        for connection in connections:
            oi.connect_events_to_objects(column_ids=connection["column_ids"],
                                         labels=connection["labels"] if "labels" in connection else None,
                                         keys=connection["keys"] if "keys" in connection else None)
            if "keys" in connection:
                all_keys.update(connection["keys"])
            all_keys.update(connection["column_ids"])
        oi.remove_key_props_from_events(list(all_keys))

        oi.merge_similar_states(similar_keys=["created_date_time"])

        rel_connections = [
            {
                "from": ["Payment"],
                "to": ["Invoice"],
                "relation": "PAYMENT_OF"
            },
            {
                "from": ["InvoiceLine"],
                "to": ["Invoice"],
                "relation": "BELONGS_TO"
            },
            {
                "from": ["OrderLine"],
                "to": ["Order"],
                "relation": "BELONGS_TO"
            },
            {
                "from": ["Goodsreceiptsline"],
                "to": ["Goodsreceipts"],
                "relation": "BELONGS_TO"
            },
            {
                "from": ["Goodsreceipts"],
                "to": ["Order"],
                "relation": "RECEIPT_OF"
            }
        ]

        for connection in rel_connections:
            oi.create_rels(_from=connection["from"],
                           _to=connection["to"],
                           _relation=connection["relation"])

    def finish_and_save(self):
        performance = self.modules.get_performance()
        performance.finish_and_save()

    def print_statistics(self):
        db_manager = self.modules.get_db_manager()
        db_manager.print_statistics()

    def close_connection(self):
        db_connection = self.modules.get_db_connection()
        db_connection.close_connection()

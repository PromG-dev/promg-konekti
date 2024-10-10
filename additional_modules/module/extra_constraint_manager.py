from additional_modules.queries.constraint_query import ExtraConstraintsQueryLibrary as cql


class ConstraintsManager:
    def __init__(self, db_connection):
        self.connection = db_connection

    def set_event_id_constraint(self, event_id="sysId"):
        self.connection.exec_query(cql.set_event_id_constraint,
                                   **{
                                       "event_id_name": event_id
                                   })

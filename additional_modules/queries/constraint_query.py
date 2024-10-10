from promg import Query


class ExtraConstraintsQueryLibrary:

    @staticmethod
    def set_event_id_constraint(event_id_name):
        query_str = '''
            CREATE RANGE INDEX event_sys_id_index 
            IF NOT EXISTS FOR (e:Event) ON (e.$event_id_name)
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "event_id_name": event_id_name
                     })

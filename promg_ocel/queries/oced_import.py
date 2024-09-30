from promg import Query


class OcedImportQueryLibrary:

    @staticmethod
    def get_import_object_nodes_query(file_name):
        query_str = '''
        CALL apoc.periodic.iterate(
            'CALL apoc.load.json("file://$file_name")
            YIELD value
            RETURN value',
            'MERGE (n:Entity {sysId: value.id})
            WITH n, value
            CALL apoc.create.addLabels(n, [value.name]) YIELD node AS n_labeled
            SET n_labeled += COALESCE(value.attributes, {})',
            {batchSize: 10000})            
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name
                     })

    @staticmethod
    def get_import_event_nodes_query(file_name):
        query_str = '''
        CALL apoc.periodic.iterate(
            'CALL apoc.load.json("file://$file_name")
             YIELD value
            RETURN value',
            'MERGE (a:Activity {activity: value.name})
             MERGE (e:Event {sysId: value.id, timestamp: datetime(value.timestamp)})
             MERGE (a) - [:OBSERVED] -> (e)
             SET e += COALESCE(value.eventattributes, {})
             WITH e, value
             UNWIND value.objectinstances as object_id
             MATCH (n:Entity {sysId: object_id})
             MERGE (e)-[:CORR]->(n)
             ',
            {batchSize: 10000})            
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name
                     })

    @staticmethod
    def get_create_relations_between_objects_query(file_name):
        query_str = '''
        CALL apoc.periodic.iterate(
            'CALL apoc.load.json("file://$file_name")
            YIELD value
            RETURN value',
            'MATCH (from:Entity {sysId: value.edge_pair[0]})
             MATCH (to:Entity {sysId: value.edge_pair[1]})
             WHERE value.type_pair[0] in labels(from) AND value.type_pair[1] in labels(to)
             MERGE (from) - [:REL] -> (to)
             ',
            {batchSize: 10000})            
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name
                     })

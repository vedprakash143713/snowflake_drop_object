import logging
import sys
import pandas as pd

class GetDbObject:
    def __init__(self, snow_connect):
        self.cs, ctx = snow_connect.connect_snow()
        self.logger = logging.getLogger(__name__)

    def execute_query(self, query_to_excute):
        self.logger.info(f''' {query_to_excute}''')
        query_id = self.cs.execute(f''' {query_to_excute} ''').sfqid
        result_set = pd.DataFrame(self.cs.execute("select \"name\" as warehouse_name from table(result_scan('{query_id}'))".
                                     format(query_id=query_id)).fetch_pandas_all())
        if result_set.empty:
            return 1
        else:
            self.logger.info(result_set.all())
            return 0

    def get_database_name(self, database_name):
        self.logger.info(''' Checking for Database existence''')
        get_db_details = self.execute_query(f'''SHOW DATABASES LIKE '%{database_name}%' ''')
        if get_db_details == 1:
            self.logger.info(f'''No Database exist with this name {database_name}''')

        else:
            self.logger.info(f'''{get_db_details}''')
            return 0

import logging

import snowflake.connector


class ConnectSnowDb:
    def __init__(self, snow_user, snow_password, snow_db_account, snow_role, snow_warehouse):
        self.snow_user = snow_user
        self.snow_password = snow_password
        self.snow_db_account = snow_db_account
        self.snow_role = snow_role
        self.snow_warehouse = snow_warehouse
        self.logger = logging.getLogger('info_log')

    def connect_snow(self):
        ctx = snowflake.connector.connect(
            user=self.snow_user,
            password=self.snow_password,
            account=self.snow_db_account,
            warehouse=self.snow_warehouse,
            role=self.snow_role
        )
        cs = ctx.cursor()
        return cs, ctx

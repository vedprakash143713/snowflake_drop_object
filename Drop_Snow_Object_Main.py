import argparse
import sys

sys.path.append("c:\\Users\\ved.prakash\\Documents\\gdw_development_new")
import logging
from Drop_Snow_DB_Object.snow_connect import *
from Drop_Snow_DB_Object.Get_DB_Object import *

parser = argparse.ArgumentParser(description='This is set to pick up Command line argument')
parser.add_argument('-snow_user_name', action='store', dest='snow_user_name',
                    help='Store UserName for snowflake connection', required=True)
parser.add_argument('-snow_db_account', action='store', dest='snow_db_account',
                    help='The snowflake account name in format <account_name>.<region_name>', required=True)
parser.add_argument('-snow_password', action='store', dest='snow_password',
                    help='The PASSWORD for the snowflake user', required=True)
parser.add_argument('-snow_role', action='store', dest='snow_role',
                    help='Enter the role to connect to the snowflake account mostly Sysadmin', required=True)
parser.add_argument('-snow_warehouse', action='store', dest='snow_warehouse',
                    help='Enter the snowflake warehouse which will be used in the destroy process', required=True)

results = parser.parse_args()
snow_user_name = results.snow_user_name
snow_db_account = results.snow_db_account
snow_password = results.snow_password
snow_role = results.snow_role
snow_warehouse = results.snow_warehouse


def configure_logging():
    import logging.config
    import pkg_resources
    import yaml
    try:
        logging_conf = pkg_resources.resource_string(__name__, '/Config/logging.yml')
        logging.config.dictConfig(yaml.safe_load(logging_conf))
        logging.basicConfig(level=logging.INFO)
    except Exception as e:
        print(e)
        print("Error in reading logging file config")


if __name__ == '__main__':
    if sys.version_info < (3, 0):
        raise Exception("Snowflake module requires Python 3. Current version: %s", sys.version_info)
    else:
        print(f"Current version:{sys.version_info}")
    # Initialise the logger
    configure_logging()

    logging.info('''
                    ################################################################################################
                    ################################################################################################
                    ################################################################################################
                            This utility drops all the object with the database named  passed
                    ################################################################################################
                    ################################################################################################
                    ''')
    logging.info(''' The Job is ready to take argument input to connect to the database ''')
    logging.info(f'{snow_user_name},{snow_db_account},{snow_password},{snow_role},{snow_warehouse}')
    snow_connect = ConnectSnowDb(snow_user_name, snow_password, snow_db_account, snow_role, snow_warehouse)
    cs, ctx = snow_connect.connect_snow()
    get_object_list = GetDbObject(snow_connect)

    try:
        if cs.execute("select current_role()").fetchone() == 'SYSADMIN':
            logging.info('''Role is SYSADMIN''')
        else:
            logging.error(''' Role is not SYSADMIN''')
    except snowflake.connector.errors.ProgrammingError as e:
        logging.error('''Error connecting to snowflake''')
        sys.exit()

    logging.info(''' Please provide the DATABASE NAME ''')
    database_name = input('Enter the Database name to be cleaned:=========: ')

    # Recursive loop for dropping object
    while database_name:
        if database_name in 'PROD':
            logging.info(f'''############################# 
                                     Ensure that the DB you dropping is not production.
                                     The current enter value of DB is {database_name} 
                                     This script do not drop DB Object with the PROD Name in it.
                                     The script exit here.
                         #############################''')
            sys.exit()
        else:
            if get_object_list.get_database_name(database_name) == 0:
                logging.info(f'''###################################
                                    Listing below object associated object with the database {database_name}.
                                ###################################''')

        logging.info('''Below Is the List of Warehouse associated with the DATABASE''')
        Query_id_list_of_warehouse = cs.execute(
            "SHOW WAREHOUSES LIKE '%{database_name}%'".format(database_name=database_name)).sfqid
        list_of_warehouse = cs.execute("select \"name\" as warehouse_name from table(result_scan('{query_id}'))".
                                       format(query_id=Query_id_list_of_warehouse)).fetch_pandas_all()
        logging.info(f'''
                        {list_of_warehouse}
                     ''')
        drop_warehouse = input("Enter Yes or No to drop the warehouse:")
        if drop_warehouse == 'Yes':
            logging.info("################################ DROPPING WAREHOUSE ####################################")
            for warehouse_index, warehouse in list_of_warehouse.iterrows():
                try:
                    logging.info("DROP WAREHOUSE IF EXISTS {warehouse_name}".format(warehouse_name=warehouse[0]))
                    cs.execute("DROP WAREHOUSE IF EXISTS {warehouse_name}".format(warehouse_name=warehouse[0]))
                    logging.info(cs.fetchone())
                except snowflake.connector.errors.ProgrammingError as e:
                    logging.error(e)
                    sys.exit()
            logging.info(f'''################################ 
                            All WAREHOUSE for the DATABASE {database_name} is dropped "
                            ###################################''')
        else:
            logging.info('''####################### Re trigger the job and chose correct database ################''')
            sys.exit()

        logging.info('''Below Is the List of User  with the DATABASE NAME''')
        logging.info('''Switch role to security Admin''')
        cs.execute("Use ROLE SECURITYADMIN")
        cs.execute(f"use warehouse {snow_warehouse}")
        Query_id_list_of_User = cs.execute(
            "SHOW USERS LIKE '%{database_name}%'".format(database_name=database_name)).sfqid
        list_of_user = cs.execute("select \"name\" as User_Name from table(result_scan('{query_id}'))".
                                  format(query_id=Query_id_list_of_User)).fetch_pandas_all()
        logging.info(f'''
                        {list_of_user}
                     ''')
        drop_user = input("Enter Yes or No to drop the USER:")
        if drop_user == 'Yes':
            logging.info("################################ DROPPING USER ####################################")
            for users_id, users in list_of_user.iterrows():
                try:
                    logging.info("DROP USER IF EXISTS {users}".format(users=users[0]))
                    cs.execute("DROP USER IF EXISTS {users}".format(users=users[0]))
                    logging.info(cs.fetchone())
                except snowflake.connector.errors.ProgrammingError as e:
                    logging.error(e)
                    sys.exit()
            logging.info(
                f"################################ USER with the {database_name} dropped ####################################")
        else:
            logging.info("####################### Re trigger the job and chose correct database ################")
            sys.exit()

        logging.info("Below Is the List of ROLES  with the DATABASE NAME")
        cs.execute("Use ROLE SECURITYADMIN")
        cs.execute(f"use warehouse {snow_warehouse}")
        Query_id_list_of_Roles = cs.execute(
            "SHOW ROLES LIKE '%{database_name}%'".format(database_name=database_name)).sfqid
        list_of_roles = cs.execute("select \"name\" as User_Name from table(result_scan('{query_id}'))".
                                   format(query_id=Query_id_list_of_Roles)).fetch_pandas_all()
        logging.info(f'''
                        {list_of_roles}
                     ''')
        drop_role = input("Enter Yes or No to drop the Roles:")
        if drop_role == 'Yes':
            logging.info("################################ DROPPING Role ####################################")
            for roles_id, roles in list_of_roles.iterrows():
                try:
                    logging.info("DROP ROLE IF EXISTS {roles}".format(roles=roles[0]))
                    cs.execute("DROP ROLE IF EXISTS {roles}".format(roles=roles[0]))
                    logging.info(cs.fetchone())
                except  Exception as e:
                    print(e)
                    sys.exit()
            logging.info(
                f"################################ Roles with the {database_name} dropped ####################################")
        else:
            logging.info("####################### Re trigger the job and chose correct database ################")
            sys.exit()

        logging.info("#####About to Drop the Database##############")
        cs.execute("Use ROLE SYSADMIN")
        drop_database = input(f"Enter Yes or No to drop the Database {database_name}:")
        if drop_database == 'Yes':
            logging.info(f"DROP DATABASE IF EXISTS {database_name}")
            cs.execute(f"DROP DATABASE IF EXISTS {database_name}")
            logging.info(cs.fetchone())
            database_name = input('Enter the Database name to be cleaned:=========: ')
        else:
            logging.info("####################### Re trigger the job and chose correct database ################")
            sys.exit()



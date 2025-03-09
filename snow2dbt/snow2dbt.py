import argparse
import logging
import sys
import os
from tabulate import tabulate
import yaml
from re import sub
import snowflake.connector
from getpass import getpass
import argcomplete


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
)
logger = logging.getLogger(__name__)



#############################################################################
#
#
# Handling autocompletion of snowflake
#
#
#############################################################################

def snowflake_connector_init():
    """
    
    Return Snowflake Connector Interface 
    
    """

    profilePath = retrieve_profile_path()
    with open('./.cache', 'w+') as file:
        with open(profilePath) as profileFile:
                config = yaml.load(profileFile, Loader=yaml.FullLoader)

                if os.path.exists('./.cache'):
                    with open('./.cache', 'r') as file:
                        defaultProfile = file.read()
                else:
                    defaultProfile=0
                profile = list(config.keys())[defaultProfile]
                account = config[profile]['outputs']['dev']['account']
                username = config[profile]['outputs']['dev']['user']
                password = config[profile]['outputs']['dev']['password']

    conn = snowflake.connector.connect(
    user=username, 
    password=password,
    account=account,
    )
    return conn

def complete_table_name(prefix, parsed_args, parser, *args):
    conn = snowflake_connector_init
    available_tables = list_database(conn)
    return [table for table in available_tables if table.startswith(prefix)]

def list_database(conn: snowflake.connector.SnowflakeConnection):
    """
    Return list of database available with the profile
    """

    databaseRequest = "SHOW DATABASES;"

    cursor = conn.cursor()
    cursor.execute(databaseRequest)
    return [ row['name'] for row in cursor]


def snake_case(field: str):
    """
        Convert a field into SnakeCase
    
    """
    return '_'.join(
    sub('([A-Z][a-z]+)', r' \1',
    sub('([A-Z]+)', r' \1',
    field.replace('-', ' '))).split()).lower()

def parser_cmd():
    """
    
    Handling parsing arguments in command
    
    """
    parser = argparse.ArgumentParser(description="Generate YAML and SQL output for a Snowflake table.")
    
    subparsers = parser.add_subparsers(dest="cmd", help="help")

    profile_parser = subparsers.add_parser('profile', help="Listing profil available in your dbt env")
    profile_parser.add_argument("--list", help="Listing a dbt profile available", action="store_true")
    profile_parser.add_argument("--clear", help="Clear the cache used for dbt profile", action="store_true")
    profile_parser.add_argument("--select", help="Selecting a dbt profile to use by default")

    reverse_parser = subparsers.add_parser('reverse', help="Reversing a table in snowflake")
    reverse_parser.add_argument("--account", help="Snowflake Account ID")
    reverse_parser.add_argument("--username", help="Snowflake User ID")
    reverse_parser.add_argument("--key", help="Snowflake Private Key (For keypair authentification)")
    reverse_parser.add_argument("--profile", help="DBT profile to used for authentification (Only  in auth_mode: dbt). If no profile, it'll take the first one in yaml config", default=None)
    reverse_parser.add_argument("--target", help="Complete Snowflake table ID (<database>.<schema>.<table>)").completer = complete_table_name
    reverse_parser.add_argument("-l", "--lower", action="store_true", help="Lowercase type names in YAML file")
    reverse_parser.add_argument("--leading_comma", action="store_true", help="Leading comma")
    reverse_parser.add_argument("--snake", action="store_true", help="Convert field names to snake_case")
    reverse_parser.add_argument("--description", action="store_true",
                        help="Include empty description property in YAML file", default='')
    reverse_parser.add_argument("--prefix", help="Prefix to add to columns names", default=None)
    reverse_parser.add_argument("--suffix", help="Suffix to add to column names", default=None)
    reverse_parser.add_argument("--output", help="Output folder of scripts. By default 'target/snow2dbt'",
                            default='target/snow2dbt')
    reverse_parser.add_argument("--auth_mode", help="Snowflake Authentification used. Default: dbt: will rely on your DBT profile file.", default='dbt')
    argcomplete.autocomplete(reverse_parser)

    return parser.parse_args()


def retrieve_profile_path():
    """
    
    Trying to retireve DBT profile.yml path on a system
    
    """

    #If OS used is windows
    if os.name == 'nt':
        profiltePath = os.path.join(os.environ['USERPROFILE'],'.dbt')
    #If OS used is linux
    else:
        profiltePath = os.path.join(os.environ['HOME'],'.dbt')
    
    #Looking for profile file.yml
    for file in os.listdir(profiltePath):
        if file == 'profiles.yml':
            return os.path.join(profiltePath, 'profiles.yml')

def snow2dbt():
    """
    
    Executing serializer from Snowflake to DBT Contract
    
    """
    
    logger.info("   _____                        ___      ____  __    __ ")
    logger.info("  / ___/____  ____ _      __   |__ \    / __ \/ /_  / /_")
    logger.info("  \__ \/ __ \/ __ \ | /| / /   __/ /   / / / / __ \/ __/")
    logger.info(" ___/ / / / / /_/ / |/ |/ /   / __/   / /_/ / /_/ / /_  ")
    logger.info("/____/_/ /_/\____/|__/|__/   /____/  /_____/_.___/\__/  ")
    logger.info("\n")

    args = parser_cmd()
    subcommand = args.cmd

    #########################################
    # profile subcommand handling
    #########################################

    if subcommand == 'profile':
        profilePath = retrieve_profile_path()

        if args.select:
            with open(profilePath) as profileFile:
                config = yaml.load(profileFile, Loader=yaml.FullLoader)
                profileRows = []
                i=1
                for profile in config.keys():
                    if args.select == str(i): 
                        username = config[profile]['outputs']['dev']['user']
                        account = config[profile]['outputs']['dev']['account'] 
                        logging.info(f"You're using profile {i} - {profile} with username {username} on account {account}")
                        with open('./.cache', 'w+') as file:
                            file.write(args.select)
                        break
                    i+=1
        elif args.clear:
            if os.path.exists('./.cache'):
                os.remove('./.cache')
                logging.info("Identity cache has been clear successfully")
        else:
            if os.path.exists('./.cache'):
                with open('./.cache', 'r') as file:
                    defaultProfile = file.read()
            else:
                defaultProfile=None 

            with open(profilePath) as profileFile:
                    config = yaml.load(profileFile, Loader=yaml.FullLoader)
                    profileRows = []
                    i=1
                    for profile in config.keys():
                        profileRows.append([i, profile, config[profile]['outputs']['dev']['account'] , config[profile]['outputs']['dev']['user']  ,'X' if defaultProfile == str(i) else '' ])
                        i+=1
                    grid = tabulate(profileRows, headers=["Id", "Profile", 'Tenant', 'User',"Default Used"], tablefmt="grid")
                    logging.info(f"Total profile available: {len(config)}\n\n{grid}")


    #########################################
    # Reverse subcommand table
    #########################################
    if subcommand == 'reverse':

        if args.target:
            
            database, schema, table = args.target.split('.')
        else:
            logger.error("Invalid Snowflake table reference. <database>.<schema>.<table>")
            sys.exit(1)
        
        leadingComma = args.leading_comma

        if args.auth_mode:
            if args.auth_mode == "dbt":
                
                logger.info('Scanning profile.yml on your system')
                profilePath = retrieve_profile_path()
                logger.info(f"profiles.yml file has been found {profilePath}")
                with open(profilePath) as profileFile:
                    config = yaml.load(profileFile, Loader=yaml.FullLoader)

                    if args.profile is None:
                        if os.path.exists('./.cache'):
                            with open('./.cache', 'r') as file:
                                defaultProfile = int(file.read())-1
                        else:
                            defaultProfile=0
                        profile = list(config.keys())[defaultProfile]
                    else:
                        if args.profile not in config.keys():
                            raise ValueError('DBT Profile dosn''exist. Please fulfill a valid dbt profile.')
                        profile = args.profile
                        
                    account = config[profile]['outputs']['dev']['account']
                    username = config[profile]['outputs']['dev']['user']
                    password = config[profile]['outputs']['dev']['password']
                    #authentificator = config[profile]['outputs']['dev']['authenticator']
                    
            elif args.auth_mode == "standard":
                    if args.account is None or args.username is None: 
                        raise ValueError('Please provide a valid account name or username to login to Snowflake.')
                    
                    account = args.account
                    username = args.username
                    if args.username is not None and args.account is not None:         
                        password=getpass()
        else:
            logger.error("Invalid Auth Mode used")
            sys.exit(1)

        #Generating Snowflake Client

        conn = snowflake.connector.connect(
        user=username, 
        password=password,
        account=account,
        )

        #Getting information about table
        
        tableInfoRequest = f"""SELECT * 
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';"""
        tableInfoCursor = conn.cursor().execute(tableInfoRequest)
        tableInfoResult = tableInfoCursor.fetchone()
        tableInfoColumn = list(map(lambda x: x.name , tableInfoCursor.description))
        tableInfo = dict(zip(tableInfoColumn,tableInfoResult))

        columnInfoRequest = f"""DESCRIBE TABLE {database}.{schema}.{table};"""
        columnInfoCursor = conn.cursor().execute(columnInfoRequest)
        columnInfoColumn = list(map(lambda x: x.name , columnInfoCursor.description))
        columnInfoResult = columnInfoCursor.fetchall()

        fields = list()
        for row in columnInfoResult:
            fields.append(dict(zip(columnInfoColumn,row)))

        # Generating default model
        yaml_data = {
        "models": [
            {
                "name": tableInfo['TABLE_NAME'],
                "description": args.description,
                "config": {
                    "contract": {
                        "enforced": True
                    }
                },
                #"constraints": None,
                "columns": []
                    }
                ]
            }
        #Generating columns contracts
        columns = list()
        for field in fields:
            name = field['name']

            if args.suffix:
                name = name + args.suffix
            if args.prefix:
                name = args.prefix + name
            if args.lower:
                name = name.lower()
            if args.snake:
                name = snake_case(name)
            
            fieldset = {'name': name, 'data_type':field['type']}
            constraints = list()
            if field['primary key'] == 'Y':
                constraints.append({'type':'primary_key'})
            if field['null?'] == 'N':
                constraints.append({'type':'not_null'})
            if field['unique key'] == 'Y':
                constraints.append({'type':'unique'})
            if len(constraints) > 0:
                fieldset['constraints'] = constraints
            columns.append(fieldset)
        yaml_data['models'][0]['columns'] = columns

        # Generating YAML format
        yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)
        
        output_folder = args.output
        output_path = f"./{output_folder}/{schema}/"
        os.makedirs(output_path, exist_ok=True)

        ### Generating SQL format
        sql_columns_separator = f"\n\t," if leadingComma else f",\n\t" 
        sql_columns_statement = sql_columns_separator.join([ field['name'] for field in columns])
        sql_from_statement = f"{database}.{schema}.{table}"
        sql_output = f"SELECT\n\t{sql_columns_statement}\nFROM {{ ref('{sql_from_statement}') }}"
        sql_output+=f"\n -- Replace by a ref() or source() value"

        with open(f"{output_path}/{table}.yaml", "w", encoding="utf-8") as yaml_file:
                yaml_file.write(yaml_output)

        with open(f"{output_path}/{table}.sql", "w", encoding="utf-8") as sql_file:
                sql_file.write(sql_output)
        
        logger.info("YAML and SQL files written at path: %s", output_path)
        logger.info(f"Reversing Table {schema}.{table} into model has been completed")

if __name__ == "__main__":

    snow2dbt()
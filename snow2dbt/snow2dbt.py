import argparse
import logging
import sys
import os
import yaml
from re import sub
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
)
logger = logging.getLogger(__name__)


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
    
    parser.add_argument("--account", help="Snowflake Account ID")
    parser.add_argument("--username", help="Snowflake User ID")
    parser.add_argument("--profile", help="DBT profile to used for authentification (Only  in auth_mode: dbt). If no profile, it'll take the first one in yaml config", default=None)
    parser.add_argument("--target", help="Complete Snowflake table ID (<database>.<schema>.<table>)")
    parser.add_argument("-l", "--lower", action="store_true", help="Lowercase type names in YAML file")
    parser.add_argument("--snake", action="store_true", help="Convert field names to snake_case")
    parser.add_argument("--empty_description", action="store_true",
                        help="Include empty description property in YAML file")
    parser.add_argument("--prefix", help="Prefix to add to columns names", default=None)
    parser.add_argument("--suffix", help="Suffix to add to column names", default=None)
    parser.add_argument("--leading_comma", help="Add comma at the start if line in SQL columns ouput", action="store_true")
    parser.add_argument("--tabs", help="Indent SQL with tabs instead of spaces", action="store_true")
    parser.add_argument("--output", help="Output folder of scripts. By default 'target/snow2dbt'",
                        default='target/snow2dbt')
    parser.add_argument("--auth_mode", help="Snowflake Authentification used. Default: dbt: will rely on your DBT profile file.", default='dbt')
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

    args = parser_cmd()

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
                    profile = list(config.keys())[0]
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
                authentificator='externalbrowser'    
    else:
        logger.error("Invalid Auth Mode used")
        sys.exit(1)

    #Generating Snowflake Client

    conn = snowflake.connector.connect(
    user=username, 
    password=password,
    account=account,
    #authenticator=authentificator,
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
            "description": "",
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
    sql_output = f"SELECT\n\t{sql_columns_statement}\nFROM{sql_from_statement}"
    sql_output+=f"\n -- Replace by a ref() or source() value"

    with open(f"{output_path}/{table}.yaml", "w", encoding="utf-8") as yaml_file:
            yaml_file.write(yaml_output)

    with open(f"{output_path}/{table}.sql", "w", encoding="utf-8") as sql_file:
            sql_file.write(sql_output)
    
    logger.info("YAML and SQL files written at path: %s", output_path)
    logger.info(f"Reversing Table {schema}.{table} into model has been completed")

if __name__ == "__main__":
    snow2dbt()
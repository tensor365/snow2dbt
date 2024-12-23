import argparse
import logging
import sys
import os
import yaml
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
)
logger = logging.getLogger(__name__)


def parser_cmd():
    """
    
    Handling parsing arguments in command
    
    """
    parser = argparse.ArgumentParser(description="Generate YAML and SQL output for a Snowflake table.")
    
    parser.add_argument("account", help="Snowflake Account ID")
    parser.add_argument("username", help="Snowflake User ID")
    parser.add_argument("target", help="Complete Snowflake table ID (<database>.<schema>.<table>)")
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
    parser.add_argument("auth_mode", help="Snowflake Authentification used. Default: dbt: will rely on your DBT profile file.", default='dbt')
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

def main():
    """
    
    Executing serializer from Snowflake to DBT Contract
    
    """

    args = parser_cmd()

    if args['target']:
        database, schema, table = args['target'].split('.')
    else:
        logger.error("Invalid Snowflake table reference. <database>.<schema>.<table>")
        sys.exit(1)

    if args['auth_mode']:
        
        match args["auth_mode"]:
            case "dbt":
                logger.info('Scanning profile.yml on your system')
                profilePath = retrieve_profile_path()
                logger.info(f"profiles.yml file has been found {profilePath}")
                with open(profilePath) as profileFile:
                    config = yaml.load(profileFile, Loader=yaml.FullLoader)
            case "standard":
                if args['account'] is None or args['username'] is None: 
                    raise ValueError('Please provide a valid account name or username to login to Snowflake.')
                account = args['account']
                username = args['username']
    else:
        logger.error("Invalid Auth Mode used")
        sys.exit(1)


    # if args['prefix'] is not None:
    
    # if args['suffix']:
    #     logger.error("Invalid Snowflake table reference. <database>.<schema>.<table>")

    #Generating Snowflake Client

    conn = snowflake.connector.connect(
    user="username", 
    account="account name",
    authenticator="externalbrowser",
)

    #Getting information about table
    
    tableInfoRequest = f"""SELECT * 
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';"""
    cur = conn.cursor()
    columnInfoRequest = f"""DESCRIBE TABLE {database}.{schema}.{table};"""



    # Generating default model
    yaml_data = {
    "models": [
        {
            "name": table_id,
            "description": table_description,
            "config": {
                "contract": {
                    "enforced": True
                }
            },
            "constraints": None,
            "columns": []
                }
            ]
        }


    # Generating YAML format

    yaml_output = yaml.dump(yaml_data, default_flow_style=False, sort_keys=False)
    
    output_folder = args.output
    output_path = f"./{output_folder}/{schema}/"
    os.makedirs(output_path, exist_ok=True)
    
    with open(f"{output_path}/{table}.yaml", "w", encoding="utf-8") as yaml_file:
            yaml_file.write(yaml_output)

    
    logger.info("YAML and SQL files wrote in path: %s", output_path)
    logger.info(f"Reversing Table {schema}.{table} into model has been completed")
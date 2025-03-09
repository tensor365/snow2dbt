<p align="center" style="vertical-align:center;">
    <img src="img/dbt.png" alt="dbtIcon" width="75" height="35" margin="auto" display="block"><br>
</p>

<h1 style="text-align:center;">Snow2dbt</h1>

<h2 style="text-align:center; font-style: italic;">Command Line to reverse engineering a Snowflake Table to DBT model and SQL</h2>


Be careful this tool is under development. There's no stable release yet.

### 1. Installing 

• From pip

```bash
pip install snow2dbt

```

• From Github repository

```bash
pip install https://github.com/tensor365/snow2dbt

```
### 2. Authentication


**Dbt SSO**

If you have already a Snowflake Connection in your profile.yml , snow2dbt will use this identity to connect to your Snowflake tenant. If you've multiple identity in your profile.yml, you can specify the profile you want by using the argument: --profile . 

To avoid supply always your connection information, you can use snow2dbt profile to configure a default profile to use. By using this command, you won't need to pass a profile when reversing a table.

**Standard Authentication**

If you want to use a standard authentification to Snowflake, you can specify following arguments: 

    • Password Authentication

    ```bash 

    --account <snowflake_accountname>
    --user <snowflake_user>
    --password <snowflake_password> 

    ```

    • Keypair Authentication

    Not available yet. Incoming soon

    ```bash

    --account <snowflake_accountname>
    --key <snowflake_private_key>

    ```

### 3. Reversing a Snowflake Table into model/contract

```bash

snow2dbt reverse --target <database>.<schema>.<table>

```

**CLI arguments**

Profile Command

```bash

snow2dbt profile 

```

| Option                | Description                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------|
| `--select`            | Select default dbt account profile you want to use                                      |
| `--list`              | List all profile available in your dbt profile                                          |
| `--clear`             | Clear profile cache. Resetting your identity                                            |

Reverse Part Command

```bash

snow2dbt reverse 

```

| Option                | Description                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------|
| `--account`           | Snowflake Account Name                                                                  |
| `--username`          | Snowflake Username                                                                      |
| `--profile`           | DBT profile used to handle authentification                                             |
| `--target`            | Target table database.schema.table                                                      |
| `-l`, `--lower`       | Output type names as lowercase in YAML file                                             |
| `--snake`             | Convert field names to snake_case                                                       |
| `--prefix`            | Prefix to add to columns names (default: None)                                          |
| `--suffix`            | Suffix to add to column names (default: None)                                           |
| `--output`            | Destination folder for scripts. (default: target/bq2dbt)                                |
| `--description`       | Description of the table reversed                                                       |
| `--auth_mode`         | Authentification mode used to handle connection to Snowflake                            |
| `--leading_comma`     | Adding Leading Comma in front of each field                                             |

### 4. Road Map

#### In development

- [ ] End March 2025: adding the capability to reverse entire schema
- [ ] February/March 2025: adding the capability to generate SQL file by using Snowflake Lineage
- [X] March 2025: Adding profile cache Authentification
- [ ] March 2025: Target autocompletion in --target argument (in development)

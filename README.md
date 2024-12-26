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

snow2dbt --target <databse>.<schema>.<table>

```

**CLI arguments**

| Option                | Description                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------|
| `-l`, `--lower`       | Output type names as lowercase in YAML file                                             |
| `--snake`             | Convert field names to snake_case                                                       |
| `--prefix`            | Prefix to add to columns names (default: None)                                          |
| `--suffix`            | Suffix to add to column names (default: None)                                           |
| `--output`            | Destination folder for scripts. (default: target/bq2dbt)                                |
| `--empty_description` | Add empty description property to YAML file if field description is empty (placeholder) |
| `--tabs`              | Use tabs instead of 4 spaces in SQL file indentation                                    |
| `--leading_comma`     | Put comma at the start of line in SQL file column list instead of end of line           |
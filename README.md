<img src="img/dbt.png" alt="dbtIcon" width="100" height="45" margin="auto" display="block"><br>

# Snow2dbt


## Command Line to reverse engineering a Snowflake Table to DBT model and SQL

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


• Dbt SSO

If you have already a Snowflake Connection in your profile.yml , snow2dbt will use this identity to connect to your Snowflake tenant. If you've multiple identity in your profile.yml, you can specify the profile you want by using the argument: --profile . 

• Standard Authentication

If you want to use a standard authentification to Snowflake, you can specify following arguments: 

• Password Authentication

```bash 

--account: <snowflake_accountname>
--user: <snowflake_user>
--password: <snowflake_password> 

```

• Keypair Authentication

Incoming soon


### 3. Reversing a Snowflake Table into model/contract


```bash

snow2dbt --target <databse>.<schema>.<table>

```
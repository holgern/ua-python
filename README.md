# ua-python
ua-python is a collection of scripts to calculate the user authority for each steem account.


## Installation of packages for Ubuntu 18.04
### postgresdb
```
sudo apt-get install postgresql postgresql-contrib
```
### mongodb
```
sudo apt install -y mongodb
```

## Installation
ua-python needs the following python packages:
```
pip3 install beem dataset pymongo psycopg2-binary
```
Build the necessary python library with:
```
pip3 setup.py build
```

### Installation of the necessary databases
A postgresql and mongo database are needed.

The postgresql database is setup by:

```
su postgres
psql -c "\password"
createdb steemua
```

The mongo database is setup by:
```
mongo --port 27017
```

In the mongo console enter next commands
```
use admin
db.createUser(
  {
    user: "superAdmin",
    pwd: "admin123",
    roles: [ { role: "root", db: "admin" } ]
  })
```
Now you can access mongo using as a service with your config
```
mongo --port 27017 -u "superAdmin" -p "admin123" --authenticationDatabase "admin"
```
You need to grant access to mongo by creating new user with ReadWrite permission
```
use myAppDb #its name of your Database
db.createUser(
  {
   user: "myAppDbUser", #desired username
   pwd: "myApp123", #desired password
   roles: [ "readWrite"]
  })
```
And try to connect using this credentials mongo --port 27017 -u "myAppDbUser" -p "myApp123" --authenticationDatabase "myAppDb"

and use this user for keystone config to connect to mongo, for example
```
mongodb://myAppDbUser:myApp123@localhost:27017/myAppDb
```

## Prepare the postgres database
```
psql -d steemua -a -f sql/postgres.sql
```

## Building User authority for all user:
A `config_pg.json` and `config_mongo.json` is necessary:
### `config_pg.json`
```
{
        "databaseConnector": "postgresql://postgres:password@localhost/steemua",
        "block_diff_for_db_storage": 1200,
        "follow_db_batch_size": 5000,
        "type_as_string": false,
        "fetch_account_ids": true,
}

```
### `config_mongo.json`
```
{
        "databaseConnector": "mongodb://mongo_ua:password@localhost/steemua?authSource=steemua",
        "block_diff_for_db_storage": 1200,
        "follow_db_batch_size": 5000,
        "type_as_string": false,
        "fetch_account_ids": true
}

```

Running the following python scripts in a loop:
```
python3 streamCustomJsonOps.py
python3 build_follow_db.py
python3 init_accounts_for_ua.py
python3 ua-python/set_trusted_accounts.py
python3 build_ua3.py
python3 ua_top100.py
```


## Elastic Search Reader
- A python module used to read data from an elastic search cluster using either DSL or SQL query.
- The reader file returns a generator that needs a loop to get the data.
- The use of generator was informed by memory considerations incase the data is too bulky.
- The reader needs to files passed i.e:

#### Config file
This has sthe configurations used by the reader to fetch the data as shown below:
```YAML
start_date: 1_day_ago
end_date: yesterday
syntax: dsl # sql|dsl
paginator: scroll # scroll|point_in_time #
index: [movies]
data_field: _source
keep_alive: 5
timeout: 60
batched_reduce_size: 20
min_score: 1
size: 10000
sort:
  year:
    order: asc
query:
  match_all: {}

in case of SQL Satetments for querying
start_date: 2_days_ago
end_date: yesterday
syntax: sql
paginator: point_in_time # scroll|point_in_time #
keep_alive: 5
index: [movies]
timeout: 60
batched_reduce_size: 20
size: 10000
sort:
  year:
    order: asc
query: "SELECT * FROM movies WHERE year > 1984"

```
#### secrets file
- This is used to authenticate the client has following contents:

```YAML
host: http://127.0.0.1
port: 9200
username: username
password: mypassword 
```

### How to use:
import ElasticSearchReader from `readers` i.e

```python
    from readers import ElasticSearchReader

    # instantiate the reader with the path to config file and 
    # file holding the secrets
    READER = ElasticSearchReader(
        creds_filepath=creds_filepath, 
        config_filepath=config_filepath
    )
    for record in READER:
        # do something with data
        #  
```
### writting data to file

if you want an easy way to write the data to json file use the writers module like so:

```python
   from readers import ElasticSearchReader
   from writers import ElasticSearchReader

    READER = ElasticSearchReader(
        creds_filepath=creds_filepath, 
        config_filepath=config_filepath
    )

    LOCAL_WRITER = ElasticSearchWriter(
        bucket="base_dir_path",
        folder_path="rest_of_data_path",
        destination="local_json",
        configs={
            "profile_name": None,
            "service_account_file": None,
        },
    )

    for record in READER:
        LOCAL_WRITER.write_data(record)

```

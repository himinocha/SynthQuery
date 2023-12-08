# SynthQuery
Merging SQL and Command Prompts in Data Retrieval

## Execution
navigate to the project direcotory

> python3 main.py --help 

## Basic Functionalities:
### cre_db (create database)
### del_db (delete database)
### cre_tb (create table)

## SQL Database (csv)
### ins_cval (insert values to csv file)
    while inserting values are supported, it would be best to append rows with known columns, since appending a new column may cause significant slow down since it requires the system to to copy files in chunks to a new file. When a new value with new columns are appended, other files will be updated with NA.

## NoSQL Database (json)
### ins_jval
    python main.py ins-jval --db=test-db --table=t --values='[{"column1": "value1", "column2": "3"}]'
### del_rows_jval
    python main.py del-rows-jval --db=test-db --table=t --conditions '{"column1": "value1", "column2": "value2"}'
### project_col_jval
    python main.py project-col-jval --db=test-db --table=t --columns=column1
### filter_jval
    python main.py filter-jval --db=test-db --table=t --criteria='{"column2": "3"}'
### order_jval
    python main.py order-jval --db=test-db --table=t --fields=column2
### group_by_jval
    python main.py group-by-jval --db=test-db --table=t --field=column1
### join_jval
    python main.py join-jval --db=test-db --table1=t --table2=t2 --join-field=column1
### select_jval
    python main.py select-jval --db=test-db --table=t --where='{"id" : {"operation": "<", "value": 4}}' --groupby=column1 --orderby=column2

## File Structure
```
.
├── database
│   └── test-db
│          ├── t
│          │   └── t.json
│          └── t2
│              └── t2.json
├── Project
│      ├── static
│      │   ├── images
│      │       └── *.png
│      ├── templates
│      │        └── *.html
│      ├── csv_file.py
│      ├── json_file.py
│      └── main.py
├── csv_file.py
├── json_file.py
├── main.py
├── .gitignore
└── README.md
```
- Inside `database` dir, we have tables dir, which contains our csv/json files. Each file will be splitted into multiple files if each file size is over 3MB.
- `Project` is where our web-application is. It can be run with `python main.py` inside the Project directory.
- `csv_file.py` handles the query executions of our SQL database/tables.
- `json_file.py` handles the query executions of our NoSQL database/tables.
- `main.py` handles the CLI of our database as explained above.

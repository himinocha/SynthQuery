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
    python main.py ins-cval --db=ev --table=ev_data --values='{"VIN (1-10)": "3ZVZ4JX19K", "County": "Franklin", "City": "Pasco", "State": "WA", "Postal Code": "99301", "Model Year": "2019", "Make": "FORD", "Model": "MUSTANG MACH-E", "Electric Vehicle Type": "Battery Electric Vehicle (BEV)", "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "Eligible", "Electric Range": 270, "Base MSRP": 0, "Legislative District": 8, "DOL Vehicle ID": "456789012", "Vehicle Location": "POINT (-119.1005655 46.2395793)", "Electric Utility": "PACIFICORP||FRANKLIN PUD", "2020 Census Tract": "53021030200"}'
### del_rows
    python3 main.py del-rows --db=ev --table=ev_data --conditions='{"Make": "TESLA"}'
### update-rows
    python3 main.py update-rows --db=ev --table=ev_data --conditions='{"Make": {"originalvalue":"TOYOTA","newvalue":"TESLA"}}'
### project-col
    python3 main.py project-col --db=ev --table=ev_data --columns='Make','Model'
### filter-tb
    python3 main.py filter-tb --db=ev --table=ev_data --conditions '{"Make": {"operator": "eq", "value": "TESLA"}}'
### order_tb
    python3 main.py order-tb --db=ev --table=ev_data --column="2020 Census Tract" --ascending=F
### groupby
    python3 main.py groupby --db ev --table ev_data --column Make --agg count
### join-tb
    python3 main.py join-tb --db=ev --tbl1=ev_data --tbl2=emission_standards --column='Model Year','Model Year'
### query
    python3 main.py query --db=ev --table=ev_data --where='{"Make": {"operator": "eq", "value": "TESLA"}}' --groupby='Model' --agg=count --order_col='Base MSRP' --ascending=T --project_col='2020 Census Tract'

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

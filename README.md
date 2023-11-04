# SynthQuery
Merging SQL and Command Prompts in Data Retrieval

## Execution
navigate to the project direcotory

> python3 main.py --help 

## Basic Functionalities:

### cre_db (create database)
### del_db (delete database)
### cre_tb (create table)
### ins_cval (insert values to csv file)
    while inserting values are supported, it would be best to append rows with known columns, since appending a new column may cause significant slow down since it requires the system to to copy files in chunks to a new file. When a new value with new columns are appended, other files will be updated with NA.
### ins_jval




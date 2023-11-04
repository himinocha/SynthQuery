import click
import os
import sys
import json
import csv


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string", help="The values to insert", required=True)
def insert_jvalues(db, table, values):
    """
    Insert json values into a table in the specified database
    # for json: python main.py insert-jvalues --db=test --table=t1 --values='[{"column1": "value1", "column2": "value2"}]'

    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    # Find the table file with either .csv or .json extension
    # table_path_csv = os.path.join(db_path, f"{table}.csv")
    table_path_json = os.path.join(db_path, f"{table}.json")

    # if os.path.exists(table_path_csv):
    #     # Insert into CSV file
    #     try:
    #         values_dict = json.loads(values)
    #     except json.JSONDecodeError:
    #         click.echo("Invalid JSON string.")
    #         sys.exit(1)

    #     if not isinstance(values_dict, dict):
    #         click.echo("Values must be a dictionary for a CSV file.")
    #         sys.exit(1)

    #     with open(table_path_csv, 'a', newline='') as csvfile:
    #         fieldnames = values_dict.keys()
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    #         # If the file is empty, write the header
    #         if os.path.getsize(table_path_csv) == 0:
    #             writer.writeheader()
    #         writer.writerow(values_dict)
    #     click.echo("Values inserted successfully!")

    if os.path.exists(table_path_json):
        # Insert into JSON file
        try:
            values_list = json.loads(values)
        except json.JSONDecodeError:
            click.echo("Invalid JSON string.")
            sys.exit(1)

        if not isinstance(values_list, list):
            click.echo("Values must be a list for a JSON file.")
            sys.exit(1)

        with open(table_path_json, 'r+') as jsonfile:
            try:
                existing_data = json.load(jsonfile)
                if not isinstance(existing_data, list):
                    click.echo("Invalid table format.")
                    sys.exit(1)
                existing_data.extend(values_list)
                jsonfile.seek(0)
                json.dump(existing_data, jsonfile)
            except json.JSONDecodeError:
                click.echo("Empty JSON file, inserting values...")
                jsonfile.seek(0)
                json.dump(values_list, jsonfile)
        click.echo("Values inserted successfully!")
    else:
        click.echo("Table does not exist.")
        sys.exit(1)

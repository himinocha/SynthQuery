import click
import os
import sys
import json
import csv


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string with []", help="The values to insert", required=True)
def ins_jval(db, table, values):
    """
    Insert json values into a table in the specified database
    # for json: python main.py insert-values --db=test --table=t1 --values='[{"column1": "value1", "column2": "value2"}]'
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    # Find the table file with either .csv or .json extension
    table_path_json = os.path.join(db_path, f"{table}.json")

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
                json.dump(values_list, jsonfile, indent=4)
        click.echo("Values inserted successfully!")
    else:
        click.echo("Table does not exist.")
        sys.exit(1)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the deletion conditions as a JSON string", help="The conditions for row deletion", required=True)
def del_fields(db, table, conditions):
    """
    Delete rows from a JSON table in the specified database based on given conditions.
    e.g. --conditions [{"column1": "value1", "column2": "value2"}] [{"column1": "value1"}]
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    with open(table_path_json, 'r') as jsonfile:
        data = json.load(jsonfile)

    # Filtering rows that do not meet the conditions
    filtered_data = [row for row in data if not all(
        row.get(key) == value for key, value in conditions_dict.items())]

    # Writing the updated data back to the JSON file
    with open(table_path_json, 'w') as jsonfile:
        json.dump(filtered_data, jsonfile, indent=4)

    click.echo("Rows deleted successfully.")


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--columns", prompt="Enter the columns to select as a comma-separated list (leave empty to select all)", default='', help="The columns to project", required=False)
def project_fields(db, table, columns):
    """
    Project specified columns from a JSON table in the specified database.
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r+') as jsonfile:
        try:
            data = json.load(jsonfile)
            # Access all columns
            col_list = []
            for record in data:
                for key, value in record.items():
                    if len(columns) == 0:
                        print(data)
                        break
                    elif key in columns:
                        col_list.append({key: value})
            print(col_list)
        except json.JSONDecodeError:
            click.echo("Empty JSON file...")

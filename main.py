import click
import os
import sys
import json
import csv

@click.group()
def cli():
    pass


@click.command()
def hello():
    """
    Greet and provide the functionalities of the CLI APP
    """
    click.echo(f"Hello! Welcome to the SynthQuery!")


@click.command()
@click.option("--db", prompt="Enter name of the database", help="The name of the database", required=1)
def create_db(db):
    """
    Create a database to the user's liking
    """
    if not os.path.isdir('database'):
        os.mkdir('database')
    os.mkdir('database' + '/' + db)


@click.command()
@click.option("--db", prompt="Enter name of the database", help="The name of the database", required=1)
def delete_db(db):
    """
    Create a database to the user's liking
    """
    if os.path.isdir('database/'+db):
        os.rmdir('database/'+db)
    else:
        click.echo("database not exist")
        sys.exit(0)

@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--format", prompt="Enter the format of the table (csv/json)", type=click.Choice(['csv', 'json'], case_sensitive=False), required=True)
def create_table(db, table, format):
    """
    Create a table in the specified database
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)
    
    table_path = os.path.join(db_path, f"{table}.{format}")
    if not os.path.exists(table_path):
        with open(table_path, 'w') as f:
            f.write("")  # create an empty file
        click.echo(f"Table {table} created successfully in database {db}!")
    else:
        click.echo("Table already exists.")

# for csv: python main.py insert-values --db=test --table=t1 --values='{"column1": "value1", "column2": "value2"}' 
# for json: python main.py insert-values --db=test --table=t1 --values='[{"column1": "value1", "column2": "value2"}]'
@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string", help="The values to insert", required=True)
def insert_values(db, table, values):
    """
    Insert values into a table in the specified database
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    # Find the table file with either .csv or .json extension
    table_path_csv = os.path.join(db_path, f"{table}.csv")
    table_path_json = os.path.join(db_path, f"{table}.json")
    
    if os.path.exists(table_path_csv):
        # Insert into CSV file
        try:
            values_dict = json.loads(values)
        except json.JSONDecodeError:
            click.echo("Invalid JSON string.")
            sys.exit(1)
        
        if not isinstance(values_dict, dict):
            click.echo("Values must be a dictionary for a CSV file.")
            sys.exit(1)

        with open(table_path_csv, 'a', newline='') as csvfile:
            fieldnames = values_dict.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # If the file is empty, write the header
            if os.path.getsize(table_path_csv) == 0:
                writer.writeheader()
            writer.writerow(values_dict)
        click.echo("Values inserted successfully!")

    elif os.path.exists(table_path_json):
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

# def load_table():
cli.add_command(hello)
cli.add_command(create_db)
cli.add_command(delete_db)
cli.add_command(create_table)
cli.add_command(insert_values)
# shawn
# take filepath from the local
# cli.add_command(load_table)

# mino
# when creating tables, create subdir for each table
# edit create_table / insert_values

# To-do
# cli.add_command(delete_table)

if __name__ == '__main__':
    cli()

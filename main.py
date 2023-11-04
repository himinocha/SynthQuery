import click
import os
import sys
import json
import csv
import csv_file as cf
import json_file as jf


@click.group()
def cli():
    pass


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


cli.add_command(create_db)
cli.add_command(delete_db)
cli.add_command(create_table)
cli.add_command(cf.insert_cvalues)
cli.add_command(jf.insert_jvalues)
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

import click
import os
import sys
import json
import csv
import csv_file as cf
import json_file as jf
import shutil


@click.group()
def cli():
    pass


@click.command()
@click.option("--db", prompt="Enter name of the database", help="The name of the database", required=1)
def cre_db(db):
    """
    Create a database to the user's liking
    """
    if not os.path.isdir('database'):
        os.mkdir('database')
    else:
        click.echo("dir database exists.")

    path = os.path.join('database', db)
    os.mkdir(path)
    click.echo(f"db '{db}' created successfully!")


@click.command()
@click.option("--db", prompt="Enter name of the database", help="The name of the database", required=1)
def del_db(db):
    """
    Delete a database to the user's liking
    """
    path = os.path.join('database', db)
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        click.echo("database not exist")
        sys.exit(0)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--format", prompt="Enter the format of the table (csv/json)", type=click.Choice(['csv', 'json'], case_sensitive=False), required=True)
def cre_tb(db, table, format):
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


cli.add_command(cre_db)
cli.add_command(del_db)
cli.add_command(cre_tb)
cli.add_command(cf.ins_cval)
cli.add_command(jf.ins_jval)
cli.add_command(cf.del_rows)
cli.add_command(cf.project_col)
cli.add_command(jf.del_fields)
cli.add_command(jf.project_fields)
cli.add_command(cf.filter_tb)

if __name__ == '__main__':
    cli()

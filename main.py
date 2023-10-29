import click
import os
import sys


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


# def create_table():
# def insert_values():
# def load_table():
cli.add_command(hello)
cli.add_command(create_db)
cli.add_command(delete_db)
if __name__ == '__main__':
    cli()

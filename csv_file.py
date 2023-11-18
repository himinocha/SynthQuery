import click
import os
import sys
import json
import csv


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string", help="The values to insert", required=True)
def ins_cval(db, table, values):
    """
    Insert csv values into a table in the specified database
    # for csv: python main.py insert-values --db=test --table=t1 --values='{"column3": "value1-", "column4": "value20"}'

    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        values_dict = json.loads(values)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    # opening the the table file
    with open(table_path_csv, 'r+', newline='') as csvfile:
        # the reader will serve as a iterator and read one line at a time
        reader = csv.DictReader(csvfile)
        existing_fieldnames = reader.fieldnames or []

        # checking for new columns
        new_columns = set(values_dict.keys()) - set(existing_fieldnames)
        if not new_columns:
            writer = csv.DictWriter(csvfile, fieldnames=existing_fieldnames)
            new_row = {field: values_dict.get(
                field, 'NA') for field in existing_fieldnames}
            writer.writerow(new_row)
        else:
            # iterating over rows and process the data to add the na
            csvfile.seek(0)
            all_fieldnames = existing_fieldnames + list(new_columns)

            temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
            with open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
                writer = csv.DictWriter(
                    temp_csvfile, fieldnames=all_fieldnames)
                writer.writeheader()

                for row in reader:
                    # update new column with NA
                    row.update(
                        {new_column: 'NA' for new_column in new_columns})
                    writer.writerow(row)

                new_row = {
                    **{field: 'NA' for field in existing_fieldnames}, **values_dict}
                writer.writerow(new_row)

            # Replace the old file with the updated temp file
            os.replace(temp_table_path_csv, table_path_csv)

    click.echo("Values inserted successfully!")


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the deletion conditions as a JSON string", help="The conditions for row deletion", required=True)
def del_rows(db, table, conditions):
    """
    Delete rows from a CSV table in the specified database based on given conditions.
    e.g. --conditions {"column1": "value1", "column2": "value2"}
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
    with open(table_path_csv, 'r', newline='') as csvfile, open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(temp_csvfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if not all(row[key] == value for key, value in conditions_dict.items()):
                writer.writerow(row)

    os.replace(temp_table_path_csv, table_path_csv)
    click.echo("Rows deleted successfully.")


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--columns", prompt="Enter the columns to select as a comma-separated list (leave empty to select all)", default='', help="The columns to project", required=False)
def project_col(db, table, columns):
    """
    Project specified columns from a CSV table in the specified database.
    --columns  Column1,Column2,...,
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_csv, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        selected_columns = [col.strip() for col in columns.split(
            ',')] if columns else reader.fieldnames

        if not all(col in reader.fieldnames for col in selected_columns):
            click.echo(
                "One or more selected columns do not exist in the table.")
            sys.exit(1)

        writer = csv.DictWriter(
            sys.stdout, fieldnames=selected_columns, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            writer.writerow(row)


# update
@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the udpate conditions as a JSON string", help="The conditions for row update", required=True)
def update_rows(db, table, conditions):
    """
    Delete rows from a CSV table in the specified database based on given conditions.
    e.g. --conditions {"column1": {"originalvalue":"newvalue"},"column2":{"originalvalue":"newvalue"}}
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
    with open(table_path_csv, 'r', newline='') as csvfile, open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(temp_csvfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            update_row = False
            for column, value_map in conditions_dict.items():
                if column in row and row[column] == value_map.get("originalvalue"):
                    row[column] = value_map.get("newvalue")
                    update_row = True

            if update_row:
                writer.writerow(row)
            else:
                writer.writerow(row)

    os.replace(temp_table_path_csv, table_path_csv)
    click.echo("Rows updated successfully.")


# filter
@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the udpate conditions as a JSON string", help="The conditions for row update", required=True)
def filter_tb(db, table, conditions):
    """
    Filter rows from a CSV table in the specified database based on given conditions.
    e.g. --conditions {"column1": value1,"column2":value2}
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    with open(table_path_csv, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(
            sys.stdout, fieldnames=reader.fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            if all(row[key] == value for key, value in conditions_dict.items()):
                writer.writerow(row)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the udpate conditions as a JSON string", help="The conditions for row update", required=True)
def order_tb(db, table, columns):

# order
# groupby
# join

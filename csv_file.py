import click
import os
import sys
import json
import csv


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string", help="The values to insert", required=True)
def insert_cvalues(db, table, values):
    """
    Insert csv values into a table in the specified database
    # for csv: python main.py insert-cvalues --db=test --table=t2 --values='{"column3": "value1-", "column4": "value20"}'

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

    # Temporary file to write updated CSV content
    temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")

    with open(table_path_csv, 'r', newline='') as csvfile, \
            open(temp_table_path_csv, 'w', newline='') as temp_csvfile:

        reader = csv.DictReader(csvfile)
        existing_fieldnames = reader.fieldnames or []

        # Combine existing fieldnames with new keys from values_dict
        all_fieldnames = sorted(
            set(existing_fieldnames).union(values_dict.keys()))

        writer = csv.DictWriter(temp_csvfile, fieldnames=all_fieldnames)
        writer.writeheader()  # Write new header

        # Write existing rows while keeping file not entirely in memory
        for row in reader:
            # Replace empty values with 'NA' for consistency
            row_with_na = {k: (v if v.strip() != '' else 'NA')
                           for k, v in row.items()}
            writer.writerow(row_with_na)

        # Fill in missing data for the new row with 'NA'
        new_row = {field: values_dict.get(field, 'NA')
                   for field in all_fieldnames}
        writer.writerow(new_row)

    # Replace the old file with the updated temp file
    os.replace(temp_table_path_csv, table_path_csv)

    click.echo("Values inserted successfully!")

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
            writer.writerow(values_dict)
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

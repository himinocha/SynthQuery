import click
import os
import sys
import json
import csv
import heapq
import tempfile
import itertools
from collections import defaultdict
from operator import itemgetter


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
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def project_col(db, table, columns, save):
    """
    Project specified columns from a CSV table in the specified database.
    --columns Column1,Column2,...
    """
    db_path = os.path.join('database', db)
    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(db_path) or not os.path.exists(table_path_csv):
        click.echo("Database or table does not exist.")
        sys.exit(1)

    selected_columns = [col.strip()
                        for col in columns.split(',')] if columns else None

    output_path = os.path.join(
        db_path, table + "_project_temp" + ".csv") if save.lower() == 'yes' else None
    with open(table_path_csv, 'r', newline='') as csvfile, \
            open(output_path, 'w', newline='') if output_path else sys.stdout as output:

        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(
            output, fieldnames=selected_columns or reader.fieldnames, extrasaction='ignore')

        if not selected_columns or all(col in reader.fieldnames for col in selected_columns):
            writer.writeheader()
            for row in reader:
                writer.writerow(row)
        else:
            click.echo(
                "One or more selected columns do not exist in the table.")

    if save.lower() == 'yes':
        click.echo(f"Projected data saved to {output_path}")


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
@click.option("--conditions", prompt="Enter the update conditions as a JSON string", help="The conditions for row update", required=True)
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def filter_tb(db, table, conditions, save):
    """
    Filter rows from a CSV table in the specified database based on given conditions.
    e.g. --conditions '{"column1": "value1", "column2": "value2"}'
    --save yes
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

    output_path = os.path.join(
        db_path, table + "_filter_temp" + ".csv") if save.lower() == 'yes' else None
    with open(table_path_csv, 'r', newline='') as csvfile, \
            open(output_path, 'w', newline='') if output_path else sys.stdout as output:

        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(
            output, fieldnames=reader.fieldnames, extrasaction='ignore')
        if output_path:
            writer.writeheader()

        for row in reader:
            if all(row[key] == value for key, value in conditions_dict.items()):
                writer.writerow(row)

    if save.lower() == 'yes':
        click.echo(f"Filtered data saved to {output_path}")


ASCEDNING_OPTION = {
    'T': True,
    "True": True,
    "true": True,
    "False": False,
    "F": False,
    "false": False

}


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--column", prompt="Enter the column to order by", help="The column to order by", required=True)
@click.option("--ascending", prompt="Ascending (T/F)", default='T', help="Sorting method", required=False)
def order_tb(db, table, column, ascending):
    """
    Order a CSV table by column using external sorting.
    e.g. python3 main.py order-tb --db ev --table ev_data --column "2020 Census Tract" --ascending F
    """
    # checking validity of the database
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    # checking validity of the table
    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    # checking existence of column for ordering
    if not column:
        click.echo("No column specified for ordering.")
        sys.exit(1)

    # perform sort
    try:

        external_sort(table_path_csv, column, ASCEDNING_OPTION[ascending])
        click.echo(f"Table {table} ordered by column {column} successfully.")
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)


def external_sort(filename, column, reverse):
    '''
    perform external sort, the file will be place into temp files of chunks and sort then merged
    '''
    chunk_files = break_into_sorted_chunks(filename, column, reverse)
    merge_chunks(chunk_files, filename, column)


def break_into_sorted_chunks(filename, column, reverse):
    '''
    breaking the files into disired chunks size, default is 5000 (lines)
    '''
    chunk_size = 5000
    chunk_files = []

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        # return the iterator of the reader
        chunk = list(itertools.islice(reader, chunk_size))
        # creating the temperary files
        while chunk:
            tmpfile = tempfile.NamedTemporaryFile(
                delete=False, mode='w', newline='')
            chunk_files.append(tmpfile.name)
            writer = csv.DictWriter(tmpfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            if reverse:
                writer.writerows(sorted(chunk, key=lambda x: x[column]))
            else:
                writer.writerows(
                    sorted(chunk, key=lambda x: x[column], reverse=True))
            tmpfile.close()
            chunk = list(itertools.islice(reader, chunk_size))

    return chunk_files


def merge_chunks(chunk_files, output_file, column):
    '''
    merging the temporary sorted files
    '''
    with open(output_file, 'w', newline='') as file:
        files = [open(chunk_file, 'r', newline='')
                 for chunk_file in chunk_files]
        readers = [csv.DictReader(f) for f in files]

        writer = csv.DictWriter(file, fieldnames=readers[0].fieldnames)
        writer.writeheader()

        merged = heapq.merge(*readers, key=lambda x: x[column])
        for row in merged:
            writer.writerow(row)

        for f in files:
            f.close()
            os.remove(f.name)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--column", prompt="Enter the column to group by", help="The column to group by", required=True)
@click.option("--agg", prompt="Enter the aggregation for group by", help="The aggregation to group by", required=True)
@click.option("--project", prompt="Enter the columns to aggregate as a comma-separated list (leave empty if aggregation is count)", default='', help="The columns to project", required=False)
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def groupby(db, table, column, agg, project, save):
    db_path = os.path.join('database', db)
    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(db_path) or not os.path.exists(table_path_csv):
        click.echo("Database or table does not exist.")
        sys.exit(1)

    project_columns = [col.strip()
                       for col in project.split(',')] if project else None
    results = perform_groupby(table_path_csv, column, agg, project_columns)

    output_path = os.path.join(
        db_path, table + "_groupby_temp" + ".csv") if save.lower() == 'yes' else None

    with open(output_path, 'w', newline='') if output_path else sys.stdout as output:
        writer = csv.DictWriter(
            output, fieldnames=['Group', 'Aggregated Value'])
        if output_path:
            writer.writeheader()

        for key, value in results.items():
            writer.writerow(
                {'Group': key, 'Aggregated Value': json.dumps(value)})

    if save.lower() == 'yes':
        click.echo(f"Grouped data saved to {output_path}")


def perform_groupby(filename, group_column, agg, project_columns):
    # used defaultdict to create a dict w/o key existing, avoiding keyerror
    group_data = defaultdict(lambda: defaultdict(list))
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            for col in (project_columns if project_columns else reader.fieldnames):
                try:
                    # Convert value to float for aggregation
                    value = float(row[col])
                    group_data[row[group_column]][col].append(value)
                except ValueError:
                    if agg == "count" or project_columns:
                        continue
                    pass

    return {group: aggregate(group_vals, agg) for group, group_vals in group_data.items()}


def aggregate(group_data, agg):
    aggregated_data = {}
    for col, values in group_data.items():
        if agg == "mean":
            aggregated_data[col] = sum(values) / len(values) if values else 0
        elif agg == "min":
            aggregated_data[col] = min(values) if values else 0
        elif agg == "max":
            aggregated_data[col] = max(values) if values else 0
        elif agg == "sum":
            aggregated_data[col] = sum(values) if values else 0
        elif agg == "count":
            aggregated_data[col] = len(values)
    return aggregated_data


def external_sort_join(file_path, key_column):
    with open(file_path, 'r', newline='') as file, \
            tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False) as outfile:

        reader = csv.DictReader(file)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        sorted_rows = sorted(reader, key=lambda row: row[key_column])
        writer.writerows(sorted_rows)

        return outfile.name


def sort_merge_join(left_file, right_file, left_column, right_column):
    with open(left_file, 'r', newline='') as left, open(right_file, 'r', newline='') as right:
        left_reader = csv.DictReader(left)
        right_reader = csv.DictReader(right)

        left_iter = itertools.groupby(
            sorted(left_reader, key=itemgetter(left_column)), key=itemgetter(left_column))
        right_iter = itertools.groupby(sorted(right_reader, key=itemgetter(
            right_column)), key=itemgetter(right_column))

        result = []
        for left_key, left_group in left_iter:
            right_group = next(
                (rg for rk, rg in right_iter if rk == left_key), [])
            for left_row in left_group:
                for right_row in right_group:
                    combined_row = {**left_row, **right_row}
                    result.append(combined_row)

        return result


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--tbl1", prompt="Enter the name of the (left) table to join", help="The name of the table", required=True)
@click.option("--tbl2", prompt="Enter the name of the (right) table to join", help="The name of the table", required=True)
@click.option("--column", prompt="Enter the column to join on", help="The column to join on")
def join_tb(db, tbl1, tbl2, column):
    db_path = os.path.join('database', db)
    left_table = os.path.join(db_path, f"{tbl1}.csv")
    right_table = os.path.join(db_path, f"{tbl2}.csv")
    left_column, right_column = column.split(',')

    if not all(os.path.exists(table) for table in [left_table, right_table]):
        click.echo("One or both tables do not exist.")
        return

    # external sort
    sorted_left = external_sort_join(left_table, left_column)
    sorted_right = external_sort_join(right_table, right_column)

    # sort-merge join
    joined_data = sort_merge_join(
        sorted_left, sorted_right, left_column, right_column)

    # show
    for row in joined_data:
        click.echo(row)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--where", prompt="Enter the conditions as a JSON string", default='', help="Conditions for filtering", required=False)
@click.option("--groupby", prompt="Enter the column to group by", default='', help="Column to group by", required=False)
@click.option("--agg", prompt="Enter the aggregation for group by", default='', help="Aggregation for group by", required=False)
@click.option("--having", prompt="Enter the conditions as a JSON string After groupby", default='', help="where for group by", required=False)
@click.option("--order_column", default='', help="Column to order by", required=False)
@click.option("--project_columns", default='', help="Columns to project", required=False)
def query(db, table, where, groupby, agg, having, order_column, project_columns):
    ''''''

    if where:
        filter_tb(db, table, where, "yes")

    if groupby and agg:
        groupby(db, table + "_filter_temp", groupby,
                agg, project_columns, "yes")

    if having:
        filter_tb(db, table + "_groupby_temp", where, "yes")

    if project_columns:
        project_col(db, table + "_filter_temp", project_columns, "yes")

    if order_column:
        order_tb(db, table + "_project_temp", order_column)

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
import operator


def get_last_chunk_file(table_path, chunk_prefix):
    chunk_files = [f for f in os.listdir(
        table_path) if f.startswith(chunk_prefix)]
    if not chunk_files:
        return None, 0
    latest_chunk = max(chunk_files, key=lambda x: int(
        x.split('_')[-1].split('.')[0]))
    return latest_chunk, int(latest_chunk.split('_')[-1].split('.')[0])


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string", help="The values to insert", required=True)
def ins_cval(db, table, values, chunk_size=5000):
    '''
    python main.py ins-cval --db=ev --table=ev_data --values='{"VIN (1-10)": "3ZVZ4JX19K", "County": "Franklin", "City": "Pasco", "State": "WA", "Postal Code": "99301", "Model Year": "2019", "Make": "FORD", "Model": "MUSTANG MACH-E", "Electric Vehicle Type": "Battery Electric Vehicle (BEV)", "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "Eligible", "Electric Range": 270, "Base MSRP": 0, "Legislative District": 8, "DOL Vehicle ID": "456789012", "Vehicle Location": "POINT (-119.1005655 46.2395793)", "Electric Utility": "PACIFICORP||FRANKLIN PUD", "2020 Census Tract": "53021030200"}'
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        values_dict = json.loads(values)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    last_chunk_file, chunk_number = get_last_chunk_file(table_path, 'chunk_')
    last_chunk_file_path = os.path.join(
        table_path, last_chunk_file) if last_chunk_file else None

    if last_chunk_file_path and os.path.getsize(last_chunk_file_path) < chunk_size:
        output_file_path = last_chunk_file_path
    else:
        chunk_number += 1
        output_file_path = os.path.join(
            table_path, f'chunk_{chunk_number}.csv')

    with open(output_file_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=values_dict.keys())
        if csvfile.tell() == 0:
            writer.writeheader()
        writer.writerow(values_dict)

    click.echo(f"Values inserted successfully into {output_file_path}")


def get_chunk_files(table_path):
    return [f for f in os.listdir(table_path) if f.startswith('chunk_') and f.endswith('.csv')]


def del_rows_in_chunk(input_file, conditions_dict, output_file):
    with open(input_file, 'r', newline='') as csvfile, open(output_file, 'w', newline='') as temp_csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(temp_csvfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if not all(row[key] == value for key, value in conditions_dict.items()):
                writer.writerow(row)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the deletion conditions as a JSON string", help="The conditions for row deletion", required=True)
def del_rows(db, table, conditions):
    '''
    python3 main.py del-rows --db=ev --table=ev_data --conditions='{"Make": "TESLA"}'
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    chunk_files = get_chunk_files(table_path)
    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        temp_chunk_path = os.path.join(table_path, f"temp_{chunk}")
        del_rows_in_chunk(chunk_path, conditions_dict, temp_chunk_path)
        os.replace(temp_chunk_path, chunk_path)

    click.echo("Rows deleted successfully in all chunks.")


def project_columns_in_chunk(input_file, selected_columns, output):
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Check if output is a file path or already a file-like object
        if isinstance(output, str):
            output_file = open(output, 'w', newline='')
        else:
            output_file = output

        writer = csv.DictWriter(
            output_file, fieldnames=selected_columns or reader.fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            writer.writerow({col: row[col] for col in selected_columns})

        # Close the file if we opened it
        if isinstance(output, str):
            output_file.close()


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--columns", prompt="Enter the columns to select as a comma-separated list", default='', help="The columns to project", required=False)
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def project_col(db, table, columns, save):
    '''
    python3 main.py project-col --db=ev --table=ev_data --columns='Make','Model'
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    selected_columns = [col.strip()
                        for col in columns.split(',')] if columns else None

    chunk_files = get_chunk_files(table_path)
    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        output_file_path = os.path.join(
            table_path, f"projected_{chunk}") if save.lower() == 'yes' else None

        if output_file_path:
            project_columns_in_chunk(
                chunk_path, selected_columns, output_file_path)
        else:
            project_columns_in_chunk(chunk_path, selected_columns, sys.stdout)

    if save.lower() == 'yes':
        click.echo(f"Projected data saved in {table_path} directory.")


# update
def update_rows_in_chunk(input_file, conditions_dict, output_file):
    with open(input_file, 'r', newline='') as csvfile, open(output_file, 'w', newline='') as temp_csvfile:
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


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the update conditions as a JSON string", help="The conditions for row update", required=True)
def update_rows(db, table, conditions):
    '''
    python3 main.py update-rows --db=ev --table=ev_data --conditions='{"Make": {"originalvalue":"TOYOTA","newvalue":"TESLA"}}'
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    chunk_files = get_chunk_files(table_path)
    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        temp_chunk_path = os.path.join(table_path, f"temp_{chunk}")
        update_rows_in_chunk(chunk_path, conditions_dict, temp_chunk_path)
        os.replace(temp_chunk_path, chunk_path)

    click.echo("Rows updated successfully in all chunks.")

    # Define comparison operators
operators = {
    "gt": operator.gt,
    "lt": operator.lt,
    "ge": operator.ge,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
    "contains": lambda a, b: b in a
}


def evaluate_condition(row_value, condition):
    op = condition.get("operator", "eq")
    value = condition["value"]
    if op in ["gt", "lt", "ge", "le"]:
        return operators[op](float(row_value), float(value))
    elif op == "contains":
        return operators[op](row_value, value)
    else:  # eq, ne
        return operators[op](row_value, value)
# filter


def filter_rows_in_chunk(input_file, conditions_dict, output):
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        if isinstance(output, str):
            output_file = open(output, 'w', newline='')
            writer = csv.DictWriter(
                output_file, fieldnames=reader.fieldnames, extrasaction='ignore')
            writer.writeheader()
        else:
            writer = csv.DictWriter(
                output, fieldnames=reader.fieldnames, extrasaction='ignore')
            if output == sys.stdout:
                writer.writeheader()

        for row in reader:
            if all(evaluate_condition(row[key], cond) for key, cond in conditions_dict.items() if key in row):
                writer.writerow(row)

        if isinstance(output, str):
            output_file.close()


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the update conditions as a JSON string", help="The conditions for row update", required=True)
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def filter_tb(db, table, conditions, save):
    '''
    python3 main.py filter-tb --db=ev --table=ev_data --conditions '{"Make": {"operator": "eq", "value": "TESLA"}}'
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        print(table_path)
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    chunk_files = get_chunk_files(table_path)
    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        output_file_path = os.path.join(
            table_path, f"filtered_{chunk}") if save.lower() == 'yes' else sys.stdout

        filter_rows_in_chunk(chunk_path, conditions_dict, output_file_path)

    if save.lower() == 'yes':
        click.echo(f"Filtered data saved in {table_path} directory.")


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
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def order_tb(db, table, column, ascending, save):
    '''
    python3 main.py order-tb --db=ev --table=ev_data --column="2020 Census Tract" --ascending=F
    '''
    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)

    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    reverse = not ASCEDNING_OPTION[ascending]
    final_sorted_chunks = []
    chunk_files = get_chunk_files(table_path)

    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        sorted_chunk = sort_and_merge_chunk(chunk_path, column, reverse)
        final_sorted_chunks.append(sorted_chunk)

    if save.lower() == 'yes':
        final_output_file = os.path.join(table_path, f"{table}_sorted.csv")
        merge_chunks(final_sorted_chunks, final_output_file, column)
        click.echo(f"Sorted data saved to {final_output_file}")
    else:
        for sorted_chunk in final_sorted_chunks:
            with open(sorted_chunk, 'r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    print(','.join(row))
            os.remove(sorted_chunk)


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


def sort_chunk(input_file, column, reverse):
    sorted_file_path = input_file + "_sorted.csv"
    with open(input_file, 'r', newline='') as file, open(sorted_file_path, 'w', newline='') as outfile:
        reader = csv.DictReader(file)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        sorted_rows = sorted(
            reader, key=lambda row: row[column], reverse=reverse)
        writer.writerows(sorted_rows)
    return sorted_file_path


def merge_chunks(chunk_files, output_file, column):
    with open(output_file, 'w', newline='') as file:
        files = [open(chunk_file, 'r', newline='')
                 for chunk_file in chunk_files]
        readers = [csv.DictReader(f) for f in files]

        writer = csv.DictWriter(file, fieldnames=readers[0].fieldnames)
        writer.writeheader()

        merged = heapq.merge(*readers, key=lambda x: x[column])
        for row in merged:
            writer.writerow(row)

        # Close all the file objects
        for f in files:
            f.close()


def sort_and_merge_chunk(chunk_file, column, reverse):
    sorted_sub_chunk_files = break_into_sorted_chunks(
        chunk_file, column, reverse)
    merged_chunk_file = chunk_file + "_sorted.csv"
    merge_chunks(sorted_sub_chunk_files, merged_chunk_file, column)
    return merged_chunk_file


def group_and_aggregate_chunk(chunk_file, group_column, agg):
    group_data = defaultdict(lambda: defaultdict(list))
    with open(chunk_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                value = float(row[group_column])
                group_data[row[group_column]][group_column].append(value)
            except ValueError:
                if agg == "count":
                    group_data[row[group_column]][group_column].append(
                        1)  # For count, just add 1
    return group_data


def merge_group_data(all_group_data):
    merged_data = defaultdict(lambda: defaultdict(list))
    for group_data in all_group_data:
        for group, values in group_data.items():
            for col, vals in values.items():
                merged_data[group][col].extend(vals)
    return merged_data


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--column", prompt="Enter the column to group by", help="The column to group by", required=True)
@click.option("--agg", prompt="Enter the aggregation for group by", help="The aggregation to group by", required=True)
@click.option("--save", prompt="Save the output to a file? (yes/no)", default='no', help="Whether to save the output to a file", required=False)
def groupby(db, table, column, agg, save):
    db_path = os.path.join('database', db)
    '''
    python3 main.py groupby --db ev --table ev_data --column Make --agg count
    '''
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path = os.path.join(db_path, table)
    if not os.path.exists(table_path):
        click.echo("Table does not exist.")
        sys.exit(1)

    chunk_files = get_chunk_files(table_path)
    if not chunk_files:
        click.echo("No chunk files found in the specified table.")
        sys.exit(1)

    db_path = os.path.join('database', db)
    table_path = os.path.join(db_path, table)
    all_group_data = []

    chunk_files = get_chunk_files(table_path)
    for chunk in chunk_files:
        chunk_path = os.path.join(table_path, chunk)
        group_data = group_and_aggregate_chunk(chunk_path, column, agg)
        all_group_data.append(group_data)

    merged_group_data = merge_group_data(all_group_data)
    final_results = {group: aggregate(vals, agg)
                     for group, vals in merged_group_data.items()}

    fieldnames = ['Group'] + \
        list(set(k for v in final_results.values() for k in v.keys()))

    if save.lower() == 'yes':
        output_path = os.path.join(db_path, table + "_groupby_temp.csv")
        with open(output_path, 'w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for key, value in final_results.items():
                row = {'Group': key, **value}
                writer.writerow(row)

        click.echo(f"Grouped data saved to {output_path}")
    else:
        writer = csv.DictWriter(
            sys.stdout, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for key, value in final_results.items():
            row = {'Group': key, **value}
            writer.writerow(row)


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


def process_chunk_pair(left_chunk, right_chunk, left_column, right_column):
    sorted_left = external_sort_join(left_chunk, left_column)
    sorted_right = external_sort_join(right_chunk, right_column)
    return sort_merge_join(sorted_left, sorted_right, left_column, right_column)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--tbl1", prompt="Enter the name of the (left) table to join", help="The name of the table", required=True)
@click.option("--tbl2", prompt="Enter the name of the (right) table to join", help="The name of the table", required=True)
@click.option("--column", prompt="Enter the column to join on", help="The column to join on")
def join_tb(db, tbl1, tbl2, column):
    '''
    python3 main.py join-tb --db=ev --tbl1=ev_data --tbl2=emission_standards --column='Model Year','Model Year'
    '''
    db_path = os.path.join('database', db)
    left_table_path = os.path.join(db_path, tbl1)
    right_table_path = os.path.join(db_path, tbl2)
    left_column, right_column = column.split(',')

    left_chunks = get_chunk_files(left_table_path)
    right_chunks = get_chunk_files(right_table_path)

    if not (left_chunks and right_chunks):
        click.echo("One or both tables do not have chunk files.")
        return

    # Assuming each table has an equal number of chunks,
    # or logic needs to be adjusted for unequal chunk counts
    for left_chunk, right_chunk in zip(left_chunks, right_chunks):
        left_chunk_path = os.path.join(left_table_path, left_chunk)
        right_chunk_path = os.path.join(right_table_path, right_chunk)

        joined_data = process_chunk_pair(
            left_chunk_path, right_chunk_path, left_column, right_column)
        for row in joined_data:
            click.echo(row)


# =======================================================


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--where", default='', help="Conditions for filtering", required=False)
@click.option("--groupby", default='', help="Column to group by", required=False)
@click.option("--agg", default='', help="Aggregation for group by", required=False)
@click.option("--having", default='', help="where for group by", required=False)
@click.option("--order_col", default='', help="Column to order by", required=False)
@click.option("--ascending", default='T', help="ascending (T/F)", required=False)
@click.option("--project_col", default='', help="Columns to project", required=False)
def query(db, table, where, groupby, agg, having, order_col, ascending, project_col):
    '''
    e.g. python3 main.py query --db=ev --table=ev_data --where='{"Make": {"operator": "eq", "value": "TESLA"}}' --groupby='Model' --agg=count --order_col='Base MSRP' --ascending=T --project_col='2020 Census Tract'
    '''

    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(where)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    if where:

        output_path = os.path.join(
            db_path, table + "_filter_temp" + ".csv")

        with open(table_path_csv, 'r', newline='') as csvfile, \
                open(output_path, 'w', newline='') if output_path else sys.stdout as output:

            reader = csv.DictReader(csvfile)
            writer = csv.DictWriter(
                output, fieldnames=reader.fieldnames, extrasaction='ignore')
            if output_path:
                writer.writeheader()

            for row in reader:
                if all(evaluate_condition(row[key], cond)
                       for key, cond in conditions_dict.items() if key in row):
                    writer.writerow(row)
        table_path_csv = output_path
    if groupby and agg:
        results = perform_groupby(table_path_csv, groupby, agg, None)

        fieldnames = ['Group'] + \
            list(set(k for v in results.values() for k in v.keys()))

        output_path = os.path.join(db_path, table + "_groupby_temp.csv")
        with open(output_path, 'w', newline='') as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for key, value in results.items():
                row = {'Group': key, **value}
                writer.writerow(row)
        table_path_csv = output_path

        if having:
            conditions_dict = json.loads(having)

            output_path = os.path.join(
                db_path, table + "_having_temp" + ".csv")
            with open(table_path_csv, 'r', newline='') as csvfile, \
                    open(output_path, 'w', newline='') if output_path else sys.stdout as output:

                reader = csv.DictReader(csvfile)
                writer = csv.DictWriter(
                    output, fieldnames=reader.fieldnames, extrasaction='ignore')
                if output_path:
                    writer.writeheader()

                for row in reader:
                    condition_met = all(evaluate_condition(
                        row[key], value) for key, value in conditions_dict.items())
                    if condition_met:
                        writer.writerow(row)
            table_path_csv = output_path
    if order_col:
        if ascending:
            external_sort(table_path_csv, order_col,
                          ASCEDNING_OPTION[ascending])
            table_path_csv = output_path

    if project_col:
        selected_columns = [col.strip()
                            for col in project_col.split(',')] if project_col else None

        output_path = os.path.join(
            db_path, table + "_project_temp" + ".csv")
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

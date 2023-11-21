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


def ins_cval(db, table, values):
    """
    Insert CSV values into a table in the specified database.
    Returns a string with the result or error message.
    # for csv: python main.py ins-cval --db=test --table=t2 --values='{"column3": "value10", "column4": "value20"}'

    """

    db_path = os.path.join('../database', db)

    if not os.path.exists(db_path):
        return "Database does not exist."

    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(table_path_csv):
        return "Table does not exist."

    with open(table_path_csv, 'r+', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        existing_fieldnames = reader.fieldnames or []

        new_columns = set(values.keys()) - set(existing_fieldnames)
        if not new_columns:
            writer = csv.DictWriter(csvfile, fieldnames=existing_fieldnames)
            new_row = {field: values.get(
                field, 'NA') for field in existing_fieldnames}
            writer.writerow(new_row)
        else:
            csvfile.seek(0)
            all_fieldnames = existing_fieldnames + list(new_columns)

            temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
            with open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
                writer = csv.DictWriter(
                    temp_csvfile, fieldnames=all_fieldnames)
                writer.writeheader()

                for row in reader:
                    row.update(
                        {new_column: 'NA' for new_column in new_columns})
                    writer.writerow(row)

                new_row = {
                    **{field: 'NA' for field in existing_fieldnames}, **values}
                writer.writerow(new_row)

            os.replace(temp_table_path_csv, table_path_csv)

    return "Values inserted successfully!"


def del_rows(db, table, conditions):
    """
    Delete rows from a CSV table in the specified database based on given conditions.
    python main.py del-rows --db=test --table=t2 --conditions='{"column3": "value10"}'
    e.g. --conditions {"column1": "value1", "column2": "value2"}
    """
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return "Database does not exist."

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        return "Table does not exist."

    temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
    with open(table_path_csv, 'r', newline='') as csvfile, open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(temp_csvfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            if not all(row[key] == value for key, value in conditions.items()):
                writer.writerow(row)

    os.replace(temp_table_path_csv, table_path_csv)
    return "Rows deleted successfully."


def project_col(db, table, columns):
    """
    Project specified columns from a CSV table in the specified database.
    --columns Column1,Column2,...
    python3 main.py project-col --db=ev --table=ev_data_small --columns='Make','Model'
    """
    db_path = os.path.join('../database', db)
    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(db_path) or not os.path.exists(table_path_csv):
        return "Database or table does not exist."

    selected_columns = [col.strip() for col in columns] if columns else None
    with open(table_path_csv, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        if not selected_columns or all(col in reader.fieldnames for col in selected_columns):
            projected_data = [{col: row[col]
                               for col in selected_columns} for row in reader]
            print(projected_data)
            return projected_data
        else:
            return "One or more selected columns do not exist in the table."


# update
def update_rows(db, table, conditions):
    """
    Delete rows from a CSV table in the specified database based on given conditions.
    e.g. --conditions {"column1": {"originalvalue":"newvalue"},"column2":{"originalvalue":"newvalue"}}
    python3 main.py update-rows --db=test --table=t2 --conditions='{"column1": {"originalvalue": "value1", "newvalue": "1000"}}'
    """
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return "Database does not exist."

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        return "Table does not exist."

    temp_table_path_csv = os.path.join(db_path, f"{table}_temp.csv")
    with open(table_path_csv, 'r', newline='') as csvfile, open(temp_table_path_csv, 'w', newline='') as temp_csvfile:
        reader = csv.DictReader(csvfile)
        writer = csv.DictWriter(temp_csvfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            update_row = False
            for column, value_map in conditions.items():
                if column in row and row[column] == value_map.get("originalvalue"):
                    row[column] = value_map.get("newvalue")
                    update_row = True

            writer.writerow(row)

    os.replace(temp_table_path_csv, table_path_csv)
    return "Rows updated successfully."


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


def filter_tb(db, table, conditions):
    """
    Filter rows from a CSV table in the specified database based on given conditions.
    python3 main.py filter-tb --db=ev --table=ev_data_small --conditions='{"Make": {"operator": "eq", "value": "TESLA"}}'
    e.g. --conditions '{"column_name": {"operator": "gt", "value": 100}}'
    --save yes
    """
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return "Database does not exist."

    table_path_csv = os.path.join(db_path, f"{table}.csv")
    if not os.path.exists(table_path_csv):
        return "Table does not exist."

    filtered_data = []
    with open(table_path_csv, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            if all(evaluate_condition(row.get(key, ""), cond)
                   for key, cond in conditions.items()):
                filtered_data.append(row)

    return filtered_data


ASCEDNING_OPTION = {
    'T': True,
    "True": True,
    "true": True,
    "False": False,
    "F": False,
    "false": False

}


def order_tb(db, table, column, ascending):
    """
    Order a CSV table by column using external sorting.
    e.g. python3 main.py order-tb --db=ev --table=ev_data_small --column='2020 Census Tract' --ascending=F
    """
    db_path = os.path.join('../database', db)
    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(db_path) or not os.path.exists(table_path_csv):
        return "Database or table does not exist."

    ascending_bool = ASCEDNING_OPTION.get(ascending, True)
    return external_sort(table_path_csv, column, ascending_bool)


def external_sort(filename, column, ascending):
    '''
    perform external sort, the file will be place into temp files of chunks and sort then merged
    '''
    chunk_files = break_into_sorted_chunks(filename, column, ascending)
    return merge_chunks(chunk_files, column)


def break_into_sorted_chunks(filename, column, ascending):
    '''
    breaking the files into disired chunks size, default is 5000 (lines)
    '''
    chunk_size = 5000
    sorted_chunks = []

    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        chunk = list(itertools.islice(reader, chunk_size))
        while chunk:
            sorted_chunk = sorted(
                chunk, key=lambda x: x[column], reverse=not ascending)
            sorted_chunks.append(sorted_chunk)
            chunk = list(itertools.islice(reader, chunk_size))

    return sorted_chunks


def merge_chunks(chunk_files, column):
    '''
    merging the temporary sorted files
    '''
    sorted_data = []
    for chunk in chunk_files:
        sorted_data.extend(chunk)

    return sorted(sorted_data, key=lambda x: x[column])


def groupby(db, table, column, agg):
    '''
    python3 main.py groupby --db=ev --table=ev_data_small --column='Make' --agg=count
    '''
    db_path = os.path.join('../database', db)
    table_path_csv = os.path.join(db_path, f"{table}.csv")

    if not os.path.exists(db_path) or not os.path.exists(table_path_csv):
        return "Database or table does not exist."

    results = perform_groupby(table_path_csv, column, agg, None)

    # Preparing the final list of dictionaries
    final_results = []
    for key, value in results.items():
        row = {'Group': key, **value}
        final_results.append(row)

    return final_results


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


def join_tb(db, tbl1, tbl2, column):
    """
    Join two  tables based on key and return the joined data.
    python3 main.py join-tb --db=ev --tb1=ev_data_small --tb2=emission_standards --column='Model Year','Model Year'
    """
    db_path = os.path.join('../database', db)
    left_table = os.path.join(db_path, f"{tbl1}.csv")
    right_table = os.path.join(db_path, f"{tbl2}.csv")
    left_column, right_column = column.split(',')

    if not all(os.path.exists(table) for table in [left_table, right_table]):
        return "One or both tables do not exist."

    sorted_left = external_sort_join(left_table, left_column)
    sorted_right = external_sort_join(right_table, right_column)

    joined_data = sort_merge_join(
        sorted_left, sorted_right, left_column, right_column)

    os.remove(sorted_left)
    os.remove(sorted_right)

    return joined_data
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
    e.g. python3 main.py query --db ev --table ev_data --where '{"Make": {"operator": "eq", "value": "TESLA"}}' --groupby Model --agg count --order_col 'Base MSRP' --ascending T --project_col '2020 Census Tract'
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

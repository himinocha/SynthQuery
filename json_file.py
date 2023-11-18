import click
import os
import sys
import json
from collections import defaultdict


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--values", prompt="Enter the values as a JSON string with []", help="The values to insert", required=True)
def ins_jval(db, table, values):
    """
    Insert json values into a table in the specified database
    Example: python main.py ins-jval --db=test-db --table=t --values='[{"column1": "value1", "column2": "3"}]'
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")

    if os.path.exists(table_path_json):
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
                # Generate new IDs
                last_id = max((record.get('id', 0)
                              for record in existing_data), default=0)
                for record in values_list:
                    last_id += 1
                    record['id'] = last_id
                existing_data.extend(values_list)
                jsonfile.seek(0)
                jsonfile.truncate()
                json.dump(existing_data, jsonfile, indent=4)
            except json.JSONDecodeError:
                click.echo("Empty JSON file, inserting values...")
                # Assign IDs for the first set of records
                for i, record in enumerate(values_list, start=1):
                    record['id'] = i
                jsonfile.seek(0)
                json.dump(values_list, jsonfile, indent=4)
        click.echo("Values inserted successfully!")
    else:
        click.echo("Table does not exist.")
        sys.exit(1)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--conditions", prompt="Enter the deletion conditions as a JSON string", help="The conditions for row deletion", required=True)
def del_rows_jval(db, table, conditions):
    """
    Delete rows from a JSON table in the specified database based on given conditions.
    e.g. python main.py del-rows --db=test-db --table=t --conditions '{"column1": "value1", "column2": "value2"}'
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        click.echo("Invalid JSON string.")
        sys.exit(1)

    with open(table_path_json, 'r') as jsonfile:
        data = json.load(jsonfile)

    # Filtering rows that do not meet the conditions
    filtered_data = [row for row in data if not all(
        row.get(key) == value for key, value in conditions_dict.items())]

    # Writing the updated data back to the JSON file
    with open(table_path_json, 'w') as jsonfile:
        json.dump(filtered_data, jsonfile, indent=4)

    click.echo("Rows deleted successfully.")


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option("--columns", prompt="Enter the columns to select as a comma-separated list (leave empty to select all)", default='', help="The columns to project", required=False)
def project_col_jval(db, table, columns):
    """
    Project specified columns from a JSON table in the specified database.
    e.g. python main.py project-col --db=test-db --table=t --columns=column1
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r+') as jsonfile:
        try:
            data = json.load(jsonfile)
            # Access all columns
            col_list = []
            for record in data:
                for key, value in record.items():
                    if len(columns) == 0:
                        print(data)
                        break
                    elif key in columns:
                        col_list.append({key: value})
            print(col_list)
        except json.JSONDecodeError:

            click.echo("Empty JSON file...")


def update_record(data, record_id, new_values):
    """
    Update a record in the data list by its ID with new values.
    """
    for record in data:
        if record.get('id') == record_id:
            for key, value in new_values.items():
                record[key] = value  # Manually update each field
            return True
    return False


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option('--record-id', prompt="Enter the ID of the record to update", help="ID of the record to update.")
@click.option('--new-values', prompt="Enter the new values as a JSON string", help="New values as a JSON string.")
def update_jval(db, table, record_id, new_values):
    """
    Update a record in a JSON file.
    Example: python main.py update-jval --db=test-db --table=t --record-id=1 --new-values='{"column1": "value1", "column2": "3"}'
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r+') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                click.echo("Invalid table format.")
                sys.exit(1)

            try:
                record_id = int(record_id)
                new_values = json.loads(new_values)
            except (ValueError, json.JSONDecodeError):
                click.echo("Invalid record ID or JSON format for new values.")
                sys.exit(1)

            if update_record(data, record_id, new_values):
                jsonfile.seek(0)
                jsonfile.truncate()
                json.dump(data, jsonfile, indent=4)
                click.echo("Record updated successfully.")
            else:
                click.echo("No matching record found to update.")
        except json.JSONDecodeError:
            click.echo("Invalid JSON file.")
            sys.exit(1)


def filter_records(data, criteria):
    """
    Filter records in the data list based on the given criteria.
    """
    filtered_data = []
    for record in data:
        if all(record.get(key) == value for key, value in criteria.items()):
            filtered_data.append(record)
    return filtered_data


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option('--criteria', prompt="Enter the filter criteria as a JSON string", help="Filter criteria as a JSON string.")
def filter_jval(db, table, criteria):
    """
    Filter records in a JSON file based on provided criteria.
    e.g. python main.py filter-jval --db=test-db --table=t --criteria='{"column2": 3}'
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                click.echo("Invalid table format.")
                sys.exit(1)

            try:
                criteria_dict = json.loads(criteria)
            except json.JSONDecodeError:
                click.echo("Invalid JSON format for criteria.")
                sys.exit(1)

            filtered_data = filter_records(data, criteria_dict)
            if filtered_data:
                click.echo(json.dumps(filtered_data, indent=4))
            else:
                click.echo("No matching records found.")
        except json.JSONDecodeError:
            click.echo("Invalid JSON file.")
            sys.exit(1)


def sort_records(data, fields):
    """
    Sort records in the data list based on the specified fields.
    """
    return sorted(data, key=lambda x: tuple(x[field] for field in fields))


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option('--fields', prompt="Enter the fields to sort by, separated by commas", help="Fields to sort by.")
def order_jval(db, table, fields):
    """
    Sort records in a JSON file based on specified fields.
    e.g. python main.py order-jval --db=test-db --table=t --fields=column2
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                click.echo("Invalid table format.")
                sys.exit(1)

            sort_fields = [field.strip() for field in fields.split(',')]
            sorted_data = sort_records(data, sort_fields)
            click.echo(json.dumps(sorted_data, indent=4))
        except json.JSONDecodeError:
            click.echo("Invalid JSON file.")
            sys.exit(1)
        except KeyError as e:
            click.echo(f"Invalid sorting field: {e}")
            sys.exit(1)


def group_by_field(data, field):
    """
    Group records in the data list based on the specified field.
    """
    grouped_data = defaultdict(list)
    for record in data:
        key = record.get(field, None)
        if key is not None:
            grouped_data[key].append(record)
    return dict(grouped_data)


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table", prompt="Enter the name of the table", help="The name of the table", required=True)
@click.option('--field', prompt="Enter the field to group by", help="Field to group by.")
def group_by_jval(db, table, field):
    """
    Group records in a JSON file based on a specified field.
    e.g. python main.py group-by-jval --db=test-db --table=t --field=column1
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        click.echo("Table does not exist.")
        sys.exit(1)

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                click.echo("Invalid table format.")
                sys.exit(1)

            grouped_data = group_by_field(data, field)
            click.echo(json.dumps(grouped_data, indent=4))
        except json.JSONDecodeError:
            click.echo("Invalid JSON file.")
            sys.exit(1)
        except KeyError:
            click.echo(f"Field '{field}' not found in records.")
            sys.exit(1)


def natural_join(data1, data2):
    """
    Perform a natural join on two sets of data.
    """
    # Identify common fields
    common_fields = set(data1[0].keys()) & set(data2[0].keys())
    joined_data = []
    for record1 in data1:
        for record2 in data2:
            if all(record1[field] == record2[field] for field in common_fields):
                joined_record = {**record1, **record2}
                joined_data.append(joined_record)
    return joined_data


@click.command()
@click.option("--db", prompt="Enter the name of the database", help="The name of the database", required=True)
@click.option("--table1", prompt="Enter the name of the first table", help="The name of the first table", required=True)
@click.option("--table2", prompt="Enter the name of the second table", help="The name of the second table", required=True)
@click.option('--join-field', prompt="Enter the join field", help="Field on which to join the tables.")
def join_jval(db, table1, table2, join_field):
    """
    Join two JSON tables in a database based on a specified field.
    e.g. python main.py join-jval --db=test-db --table1=t --table2=t2 --join-field=column1
    """
    db_path = os.path.join('database', db)
    if not os.path.exists(db_path):
        click.echo("Database does not exist.")
        sys.exit(1)

    table1_path = os.path.join(db_path, f"{table1}.json")
    table2_path = os.path.join(db_path, f"{table2}.json")

    if not os.path.exists(table1_path) or not os.path.exists(table2_path):
        click.echo("One or both tables do not exist.")
        sys.exit(1)

    with open(table1_path, 'r') as jsonfile1, open(table2_path, 'r') as jsonfile2:
        try:
            data1 = json.load(jsonfile1)
            data2 = json.load(jsonfile2)
            if not (isinstance(data1, list) and isinstance(data2, list)):
                click.echo("Invalid table formats.")
                sys.exit(1)

            joined_data = natural_join(data1, data2)
            click.echo(json.dumps(joined_data, indent=4))
        except json.JSONDecodeError:
            click.echo("Invalid JSON in one or both files.")
            sys.exit(1)
        except KeyError as e:
            click.echo(f"Error in joining tables: {e}")
            sys.exit(1)

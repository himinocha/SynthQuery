import os
import json
from collections import defaultdict

# python main.py ins-jval --db=test-db --table=t --values='[{"column1":"99","column2":"99"}]'
def ins_jval(db, table, values):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}
    values = values.strip('\'')
    
    try:
        values_list = json.loads(values.strip('\''))
        
        if not isinstance(values_list, list):
            return {"error": "Values must be a list for a JSON file."}

        with open(table_path_json, 'r+') as jsonfile:
            try:
                existing_data = json.load(jsonfile)
                if not isinstance(existing_data, list):
                    return {"error": "Invalid table format."}
                last_id = max((record.get('id', 0) for record in existing_data), default=0)
                for record in values_list:
                    last_id += 1
                    record['id'] = last_id
                existing_data.extend(values_list)
                jsonfile.seek(0)
                jsonfile.truncate()
                json.dump(existing_data, jsonfile, indent=4)
            except json.JSONDecodeError:
                for i, record in enumerate(values_list, start=1):
                    record['id'] = i
                jsonfile.seek(0)
                json.dump(values_list, jsonfile, indent=4)
        return {"message": "Values inserted successfully!"}
    except json.JSONDecodeError:
        return {"error": "Invalid JSON string."}

# python main.py del-rows-jval --db=test-db --table=t --conditions='{"column1":"99","column2":"99"}
def del_rows_jval(db, table, conditions):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}
    
    conditions = conditions.strip('\'')
    try:
        conditions_dict = json.loads(conditions)
    except json.JSONDecodeError:
        return {"error": "Invalid JSON string."}

    with open(table_path_json, 'r+') as jsonfile:
        data = json.load(jsonfile)
        filtered_data = [row for row in data if not all(row.get(key) == value for key, value in conditions_dict.items())]

    with open(table_path_json, 'w') as jsonfile:
        json.dump(filtered_data, jsonfile, indent=4)

    return {"message": "Rows deleted successfully."}

# python main.py project-col-jval --db=test-db --table=t --columns=column1,column2
def project_col_jval(db, table, columns):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            col_list = []
            if not columns:
                return data
            else:
                columns = columns.split(',')
                for record in data:
                    col_list.append({key: record[key] for key in columns if key in record})
            return col_list
        except json.JSONDecodeError:
            return {"error": "Invalid JSON file."}

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

# python main.py update-jval --db=test-db --table=t --record-id=1 --new-values='{"column1":"value1","column2":"3"}'
def update_jval(db, table, record_id, new_values):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}

    with open(table_path_json, 'r+') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                return {"error": "Invalid table format."}
            
            new_values = new_values.strip('\'')
            try:
                record_id = int(record_id)
                new_values = json.loads(new_values)
            except (ValueError, json.JSONDecodeError):
                return {"error": "Invalid record ID or JSON format for new values."}

            updated = update_record(data, record_id, new_values)
            if updated:
                jsonfile.seek(0)
                jsonfile.truncate()
                json.dump(data, jsonfile, indent=4)
                return {"message": "Record updated successfully."}
            else:
                return {"message": "No matching record found to update."}
        except json.JSONDecodeError:
            return {"error": "Invalid JSON file."}

def filter_records(data, criteria):
    """
    Filter records in the data list based on the given criteria.
    """
    filtered_data = []
    for record in data:
        if all(record.get(key) == value for key, value in criteria.items()):
            filtered_data.append(record)
    return filtered_data

# python main.py filter-jval --db=test-db --table=t --criteria='{"column2":"3"}'
def filter_jval(db, table, criteria):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}

    criteria = criteria.strip('\'')
    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                return {"error": "Invalid table format."}

            try:
                criteria_dict = json.loads(criteria)
            except json.JSONDecodeError:
                return {"error": "Invalid JSON format for criteria."}

            filtered_data = filter_records(data, criteria_dict)
            # return {"criteria_dict": criteria_dict}
            return {"data": filtered_data}
        except json.JSONDecodeError:
            return {"error": "Invalid JSON file."}

def sort_records(data, fields):
    """
    Sort records in the data list based on the specified fields.
    """
    return sorted(data, key=lambda x: tuple(x[field] for field in fields))

# python main.py order-jval --db=test-db --table=t --fields=column2
def order_jval(db, table, fields):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                return {"error": "Invalid table format."}

            sort_fields = [field.strip() for field in fields.split(',')]            
            filtered_data = [item for item in data if all(field in item for field in sort_fields)] # skip missing keys

            sorted_data = sort_records(filtered_data, sort_fields)
            return {"data": sorted_data}
        except json.JSONDecodeError:
            return {"error": "Invalid JSON file."}
        except KeyError as e:
            return {"error": f"Invalid sorting field: {e}"}
        
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

# python main.py group-by-jval --db=test-db --table=t --field=column1
def group_by_jval(db, table, field):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table_path_json = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path_json):
        return {"error": "Table does not exist."}

    with open(table_path_json, 'r') as jsonfile:
        try:
            data = json.load(jsonfile)
            if not isinstance(data, list):
                return {"error": "Invalid table format."}

            grouped_data = group_by_field(data, field)
            return {"data": grouped_data}
        except json.JSONDecodeError:
            return {"error": "Invalid JSON file."}
        except KeyError:
            return {"error": f"Field '{field}' not found in records."}

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

# python main.py join-jval --db=test-db --table1=t --table2=t2 --join-field=column1
def join_jval(db, table1, table2, join_field):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": "Database does not exist."}

    table1_path = os.path.join(db_path, f"{table1}.json")
    table2_path = os.path.join(db_path, f"{table2}.json")
    if not os.path.exists(table1_path) or not os.path.exists(table2_path):
        return {"error": "One or both tables do not exist."}

    with open(table1_path, 'r') as jsonfile1, open(table2_path, 'r') as jsonfile2:
        try:
            data1 = json.load(jsonfile1)
            data2 = json.load(jsonfile2)
            if not (isinstance(data1, list) and isinstance(data2, list)):
                return {"error": "Invalid table formats."}

            joined_data = natural_join(data1, data2)
            return {"data": joined_data}
        except json.JSONDecodeError:
            return {"error": "Invalid JSON in one or both files."}
        except KeyError as e:
            return {"error": f"Error in joining tables: {e}"}

def filter_data(data, criteria):
    """ Filter data based on criteria which can include > and < operations. """
    filtered_data = []
    for record in data:
        include_record = True
        for field, condition in criteria.items():
            if 'operation' in condition and 'value' in condition:
                operation = condition['operation']
                value = condition['value']
                # convert str to float if possible
                if value.isdigit():
                    value = float(value)
                    comp_val = float(record.get(field, 0))
                    comp_eq_val = float(record.get(field, None))
                else:
                    value = value
                    comp_val = record.get(field, 0)
                    comp_eq_val = record.get(field, None)
                
                # compare values
                if operation == '=' and not comp_eq_val == value:
                    include_record = False
                    break
                elif operation == '>' and not comp_val > value:
                    include_record = False
                    break
                elif operation == '<' and not comp_val < value:
                    include_record = False
                    break
            elif record.get(field) != condition:
                include_record = False
                break
        if include_record:
            filtered_data.append(record)
    return filtered_data

def group_by(data, field):
    """ Group data by a field """
    grouped_data = defaultdict(list)
    for record in data:
        grouped_data[record.get(field, None)].append(record)
    return dict(grouped_data)

def order_by(data, fields):
    """ Sort data by given fields """
    return sorted(data, key=lambda x: tuple(x.get(field, None) for field in fields))

# python main.py select-jval --db=test-db --table=t --where='{"id":{"operation":"<","value":4}}' --groupby=column1 --orderby=column2
def select_jval(db, table, where, groupby, orderby):
    db_path = os.path.join('../database', db)
    if not os.path.exists(db_path):
        return {"error": f"Database does not exist.{db_path}"}

    table_path = os.path.join(db_path, f"{table}.json")
    if not os.path.exists(table_path):
        return {"error": "Table does not exist."}

    with open(table_path, 'r') as file:
        try:
            data = json.load(file)
            if not isinstance(data, list):
                return {"error": "Invalid table format."}
            where = where.strip('\'')
            # return {f"{where}"}
        
            # Apply where
            if where:
                criteria = json.loads(where)
                data = filter_data(data, criteria)

            # return {"params": f"{where} {groupby} {orderby}"}                
            # Apply groupby
            if groupby:
                data = group_by(data, groupby)
            elif orderby:  # Apply orderby only if not grouped
                order_fields = [field.strip() for field in orderby.split(',')]
                data = order_by(data, order_fields)

            return {"data": data}
        except json.JSONDecodeError:
            return {"error": f"Invalid JSON format.{table_path}"}

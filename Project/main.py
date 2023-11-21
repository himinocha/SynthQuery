from flask import Flask, request, render_template
import re
import json
import json_file as jf
import csv_file as cf
import shlex
import re

# import sys
# sys.path.append('../')

app = Flask(__name__)


@app.route('/')
def base():
    return render_template('home.html', title='home')


@app.route('/home')
def home():
    return render_template('home.html', title='Home')


@app.route('/documentation')
def about():
    return render_template('documentation.html', title='Documentation')


@app.route('/demo')
def demo():
    return render_template('demo.html', title='Demo')


@app.route('/results', methods=['GET', 'POST'])
def results():
    if request.method == 'POST':
        query = request.form['query']

        # Process the query
        query_brkdwn = query.split(' ')
        func = query_brkdwn[2]
        db = query_brkdwn[3][5:]
        table = query_brkdwn[4][8:]
        # python main.py select-jval --db=test-db --table=salaries --where='{"salary_in_usd":{"operation":">","value":"400000"}}' --groupby=job_title --orderby=salary_in_usd
        if func == 'select-jval':
            # select * from t
            if len(query_brkdwn) == 5:
                result = jf.select_jval(db, table, '', '', '')
                pretty_result = json.dumps(result, indent=4)
                return render_template('results.html', query=query, result=pretty_result)
            elif len(query_brkdwn) == 6:
                where = query_brkdwn[5][8:]
                result = jf.select_jval(db, table, where, '', '')
                pretty_result = json.dumps(result, indent=4)
                return render_template('results.html', query=query, result=pretty_result)

            # when there are conditions
            where = query_brkdwn[5][8:]
            groupby = query_brkdwn[6][10:]
            orderby = query_brkdwn[7][10:]

            result = jf.select_jval(db, table, where, groupby, orderby)

        # python main.py ins-jval --db=test-db --table=salaries --values='[{"work_year":"2024","experience_level":"EX","employment_type":"FT","job_title":"Jedi_Master","salary":"1000000","salary_currency":"USD","salary_in_usd":"1000000","employee_residence":"US","remote_ratio":0,"company_location":"US","company_size":"S"}]'
        # python main.py ins-jval --db=test-db --table=salaries --values='[{"work_year":"2024","experience_level":"EX"}]
        elif func == 'ins-jval':
            values = query_brkdwn[5][9:]

            result = jf.ins_jval(db, table, values)

        # python main.py del-rows-jval --db=test-db --table=salaries --conditions='{"work_year":"2024","experience_level":"EX"}'
        # python main.py del-rows-jval --db=test-db --table=salaries --conditions='{"work_year":"2024","experience_level":"EX","employment_type":"FT","job_title":"Jedi_Master","salary":"1000000","salary_currency":"USD","salary_in_usd":"1000000","employee_residence":"US","remote_ratio":0,"company_location":"US","company_size":"S"}'
        elif func == 'del-rows-jval':
            conditions = query_brkdwn[5][13:]

            result = jf.del_rows_jval(db, table, conditions)

        # python main.py project-col-jval --db=test-db --table=salaries --columns=work_year,salary_in_usd
        elif func == 'project-col-jval':
            columns = query_brkdwn[5][10:]

            result = jf.project_col_jval(db, table, columns)

        # python main.py update-jval --db=test-db --table=salaries --record-id=1 --new-values='{"column1":"value1","column2":"3"}'
        elif func == 'update-jval':
            record_id = query_brkdwn[5][12:]
            new_values = query_brkdwn[6][13:]

            result = jf.update_jval(db, table, record_id, new_values)

        # python main.py filter-jval --db=test-db --table=salaries --criteria='{"column2":"3"}'
        elif func == 'filter-jval':
            criteria = query_brkdwn[5][11:]

            result = jf.filter_jval(db, table, criteria)

        # python main.py order-jval --db=test-db --table=salaries --fields=salary_in_usd
        elif func == 'order-jval':
            fields = query_brkdwn[5][9:]

            result = jf.order_jval(db, table, fields)

        # python main.py group-by-jval --db=test-db --table=salaries --field=job_title
        elif func == 'group-by-jval':
            field = query_brkdwn[5][8:]

            result = jf.group_by_jval(db, table, field)

        # python main.py join-jval --db=test-db --table1=t --table2=t2 --join-field=column1
        elif func == 'join-jval':
            table1 = query_brkdwn[4][9:]
            table2 = query_brkdwn[5][9:]
            join_field = query_brkdwn[6][13:]

            result = jf.join_jval(db, table1, table2, join_field)
        
        pretty_result = json.dumps(result, indent=4)
        return render_template('results.html', query=query, result=pretty_result)


@app.route('/csv_results', methods=['GET', 'POST'])
def csv_results():
    if request.method == 'POST':
        csv_query = request.form['csv_query']
        result = ''

        # Split the query
        query_parts = shlex.split(csv_query)

        if len(query_parts) >= 4:
            func = query_parts[2]
            db = query_parts[3][5:]
            table = query_parts[4][8:]

            if func == 'ins-cval':
                json_str = ' '.join(query_parts[5:])
                json_str = json_str[json_str.index('=') + 1:]

                try:
                    values = json.loads(json_str)
                    result = cf.ins_cval(db, table, values)
                except json.JSONDecodeError as e:
                    result = f"JSON decoding error: {e}"
            elif func == 'del-rows':

                conditions_str = ' '.join(query_parts[5:])
                conditions_str = conditions_str[conditions_str.index('=') + 1:]
                try:
                    conditions_dict = json.loads(conditions_str)
                    result = cf.del_rows(db, table, conditions_dict)
                except json.JSONDecodeError as e:
                    result = f"JSON decoding error: {e}"
            elif func == 'project-col':
                db = query_parts[3].split('=')[1]
                table = query_parts[4].split('=')[1]

                columns_str = query_parts[5].split('=')[1]
                columns = [col.strip().replace('\'', '')
                           for col in columns_str.split(',')]
                result = cf.project_col(db, table, columns)
            elif func == 'update-rows':
                conditions_str = ' '.join(query_parts[5:])
                conditions_str = conditions_str[conditions_str.index('=') + 1:]

                try:
                    conditions_dict = json.loads(conditions_str)
                    result = cf.update_rows(db, table, conditions_dict)
                except json.JSONDecodeError as e:
                    result = f"JSON decoding error: {e}"
            elif func == 'filter-tb':
                conditions_str = ' '.join(query_parts[5:])
                conditions_str = conditions_str[conditions_str.index('=') + 1:]

                try:
                    conditions_dict = json.loads(conditions_str)
                    result = cf.filter_tb(db, table, conditions_dict)
                except json.JSONDecodeError as e:
                    result = f"JSON decoding error: {e}"
            elif func == 'order-tb':
                db = query_parts[3].split('=')[1]
                table = query_parts[4].split('=')[1]
                column = query_parts[5].split('=')[1].strip("'")
                ascending = query_parts[6].split('=')[1]

                result = cf.order_tb(db, table, column, ascending)
            elif func == 'groupby':
                db = query_parts[3].split('=')[1]
                table = query_parts[4].split('=')[1]
                column = query_parts[5].split('=')[1].strip("'")
                agg = query_parts[6].split('=')[1]
                result = cf.groupby(db, table, column, agg)
            elif func == 'join-tb':
                db = query_parts[3].split('=')[1]
                tbl1 = query_parts[4].split('=')[1]
                tbl2 = query_parts[5].split('=')[1]
                column = query_parts[6].split('=')[1].replace('\'', '')

                result = cf.join_tb(db, tbl1, tbl2, column)
            elif func == 'query':
                where = groupby = agg = having = order_col = ascending = project_col = ''

                for part in query_parts[5:]:
                    if '--where=' in part:
                        where = part.split('=', 1)[1].replace('\'', '')
                    elif '--groupby=' in part:
                        groupby = part.split('=', 1)[1].replace('\'', '')
                    elif '--agg=' in part:
                        agg = part.split('=', 1)[1].replace('\'', '')
                    elif '--having=' in part:
                        having = part.split('=', 1)[1].replace('\'', '')
                    elif '--order_col=' in part:
                        order_col = part.split('=', 1)[1].replace('\'', '')
                    elif '--ascending=' in part:
                        ascending = part.split('=', 1)[1].replace('\'', '')
                    elif '--project_col=' in part:
                        project_col = part.split(
                            '=', 1)[1].replace('\'', '').split(',')

                result = cf.query(db, table, where, groupby, agg,
                                  having, order_col, ascending, project_col)
            else:
                result = "Invalid query format."
        else:
            result = "Invalid query format."

        return render_template('csv_results.html', query=csv_query, result=result)


if __name__ == '__main__':
    app.run(debug=True)

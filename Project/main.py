from flask import Flask, request, render_template
import re
import json
import json_file as jf

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
                return render_template('results.html', query=query, result=result)
            elif len(query_brkdwn) == 6:
                where = query_brkdwn[5][8:]
                result = jf.select_jval(db, table, where, '', '')
                return render_template('results.html', query=query, result=result)
            
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
        
        # python main.py del-rows-jval --db=test-db --table=salaries --values='[{"work_year":"2024","experience_level":"EX"}]
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

        # python main.py filter-jval --db=test-db --table=salaries --criteria='{"column2":3}'
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

        return render_template('results.html', query=query, result=result)

if __name__ == '__main__':
    app.run(debug=True)
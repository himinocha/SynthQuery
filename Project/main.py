from flask import Flask, request, render_template

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
        # Process the query here
        return render_template('results.html', query=query)

if __name__ == '__main__':
    app.run(debug=True)
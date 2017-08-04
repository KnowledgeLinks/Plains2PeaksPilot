__author__ = "Jeremy Nelson"
from flask import Flask, render_template
from flask_flatpages import FlatPages

app = Flask(__name__)
app.config['FLATPAGES_EXTENSION'] = ['md']
app.config['FLATPAGES_ROOT'] = 'doc'
pages = FlatPages(app)

@app.route("/")
def home():
    return render_template("index.html",
        pages=pages)

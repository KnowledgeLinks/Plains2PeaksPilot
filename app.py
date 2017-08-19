__author__ = "Jeremy Nelson"
from flask import Flask, render_template
from flask_flatpages import FlatPages

app = Flask(__name__)
app.config['FLATPAGES_EXTENSION'] = ['.md']
app.config['FLATPAGES_ROOT'] = 'doc'
pages = FlatPages(app)

@app.template_filter("pretty_num")
def nice_number(raw_number):
    return "{:,}".format(int(raw_number))

@app.route("/<path:name>")
def page(name):
    doc = pages.get_or_404(name)
    #doc = pages.get(name)
    return render_template("page.html", page=doc)

@app.route("/")
def home():
    total_items, total_instances = 0, 0
    for page in pages:
        total_items += int(page.meta.get("bf_items"))
        total_instances += int(page.meta.get("bf_instances"))
    print(total_items, total_instances)
    return render_template("index.html",
        pages=pages,
        total_items=total_items,
        total_instances=total_instances)

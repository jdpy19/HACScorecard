# path = app/routes.py

## External Dependencies ##
from flask import render_template
import pandas as pd

## Internal Dependencies ##
from app import app


## Body ##
@app.route("/")
@app.route("/index")
def index():
    path = ".\\hacData\\HACScorecardData\\tableauNHSNData.xlsx"
    data = pd.read_excel(path)
    return render_template("index.html",title="HAC Scorecard", tables=[data.to_html(classes="data",header=True)])

## End ##

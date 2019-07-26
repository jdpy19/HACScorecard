from HACScorecardFlask import app

@app.route("/")
@app.route("/index")
def index():
    return "HAC Scorecard"
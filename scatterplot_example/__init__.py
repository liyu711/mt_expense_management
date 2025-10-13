# app.py
from flask import Flask, render_template
import plotly as plt
import plotly.express as px
import plotly.graph_objects as go
import json

app = Flask(__name__)

@app.route('/')
def index():
    # Sample data
    df = px.data.iris()
    # fig = px.scatter(df, x="sepal_width", y="sepal_length", color="species")
    # graph_json = json.dumps(fig, cls=plt.utils.PlotlyJSONEncoder)
    print(df["sepal_width"])
    fig = go.Figure(data=go.Scatter(x=df["sepal_width"].values.tolist(), y=df["sepal_length"].values.tolist(), mode='markers'))
    graph_json = json.dumps(fig, cls=plt.utils.PlotlyJSONEncoder)
    return render_template('index.html', graph_json=graph_json)

if __name__ == '__main__':
    app.run(debug=True)
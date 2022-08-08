from flask import Flask
from flask_restful import Resource, Api, reqparse, request
import pandas
import sqlite3
import ast
import datetime
import dateutil

from settings import DB_NAME, USAGE_TABLE_NAME

app = Flask(__name__)
api = Api(app)

class Usage(Resource):

    def get(self):
        # Parse request arguments
        start = request.args.get('start', type=lambda x: dateutil.parser.parse(x), default=datetime.date.min)
        end = request.args.get('end', type=lambda x: dateutil.parser.parse(x), default=datetime.date.max) 
        limit = request.args.get('limit', type=int, default=100000)
        
        con = sqlite3.connect(DB_NAME)
        qry = f"""
        SELECT *
        FROM {USAGE_TABLE_NAME}
        WHERE update_time >= '{start}'
        AND update_time <= '{end}'
        ORDER BY update_time DESC
        LIMIT {limit}
        """
        data = pandas.read_sql(qry, con)
        con.close()

        return {'data': data.to_dict()}

api.add_resource(Usage, '/')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)

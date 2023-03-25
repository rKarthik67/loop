from flask import Flask, jsonify, request,send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
import uuid
import csv
import os


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:root@localhost:5432/loop'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Store(db.Model):
    __tablename__ = 'stores'
    id = db.Column(db.BigInteger, primary_key=True)
    timezone_str = db.Column(db.String, default='America/Chicago')
    business_hours = db.relationship('BusinessHours', backref='store', lazy=True)

    def __init__(self, id, timezone_str='America/Chicago'):
        self.id = id
        self.timezone_str = timezone_str

    def __repr__(self):
        return f'<Store {self.id}>'


class BusinessHours(db.Model):
    __tablename__ = 'business_hours'
    id = db.Column(db.BigInteger, primary_key=True)
    store_id = db.Column(db.BigInteger, db.ForeignKey('stores.id'))
    day_of_week = db.Column(db.Integer)
    start_time_local = db.Column(db.Time)
    end_time_local = db.Column(db.Time)

    def __init__(self, store_id, day_of_week, start_time_local, end_time_local):
        if store_id not in [store.id for store in Store.query.all()]:
            store_id = None
        self.store_id = store_id
        self.day_of_week = day_of_week
        self.start_time_local = start_time_local
        self.end_time_local = end_time_local

    def __repr__(self):
        return f'<BusinessHours {self.store_id} {self.day_of_week}>'



class Status(db.Model):
    __tablename__ = 'statuses'
    id = db.Column(db.BigInteger, primary_key=True)
    store_id = db.Column(db.BigInteger)
    timestamp_utc = db.Column(db.DateTime)
    status = db.Column(db.String)

    def __init__(self, store_id, timestamp_utc, status):
        self.store_id = store_id
        self.timestamp_utc = timestamp_utc
        self.status = status

    def __repr__(self):
        return f'<Status {self.store_id} {self.timestamp_utc}>'
    



@app.route('/trigger_report')
def trigger_report():
    report_id = str(uuid.uuid4())
    generate_report(report_id)
    return jsonify(report_id=report_id)


@app.route('/get_report', methods=['POST'])
def get_report():
    report_id = request.json['report_id']
    report_file = f'report_{report_id}.csv'
    if os.path.isfile(report_file):
        return send_file(report_file, as_attachment=True)
    else:
        return jsonify(status='Running')


def generate_report(report_id):
    max_timestamp_utc = get_max_timestamp_utc()
    stores = get_stores()
    report_data = []
    for store in stores:
        store_hours = get_store_hours(store)
        timezone = pytz.timezone(store.timezone_str)
        store_status = get_store_status(store.id, max_timestamp_utc)
        if store_status:
            last_status_time = store_status.timestamp_utc.astimezone(timezone)
            last_status = store_status.status
        else:
            last_status_time = None
            last_status = None
        report_data.append({'store_id': store.id, 'last_status_time': last_status_time, 'last_status': last_status,
                            **store_hours})
    
    report_df = pd.DataFrame(report_data)
    report_df['is_open'] = report_df.apply(lambda row: is_store_open(row), axis=1)
    report_df.to_csv(f'report_{report_id}.csv', index=False)
    
    
def get_max_timestamp_utc():
    max_status = Status.query.order_by(Status.timestamp_utc.desc()).first()
    if max_status:
        return max_status.timestamp_utc
    else:
        return datetime.utcnow()

def get_stores():
    return Store.query.all()

def get_store_hours(store):
    hours = {}
    if store.business_hours:
        for business_hour in store.business_hours:
            day_name = datetime.strptime(str(business_hour.day_of_week), '%w').strftime('%A')
            hours[f'{day_name}_open'] = business_hour.start_time_local.strftime('%H:%M')
            hours[f'{day_name}_close'] = business_hour.end_time_local.strftime('%H:%M')
    else:
        hours = {
            'Monday_open': '00:00',
            'Monday_close': '24:00',
            'Tuesday_open': '00:00',
            'Tuesday_close': '24:00',
            'Wednesday_open': '00:00',
            'Wednesday_close': '24:00',
            'Thursday_open': '00:00',
            'Thursday_close': '24:00',
            'Friday_open': '00:00',
            'Friday_close': '24:00',
            'Saturday_open': '00:00',
            'Saturday_close': '24:00',
            'Sunday_open': '00:00',
            'Sunday_close': '24:00'
        }
    return hours



def get_store_status(store_id, max_timestamp_utc):
    return Status.query.filter_by(store_id=store_id).filter(Status.timestamp_utc <= max_timestamp_utc).order_by(Status.timestamp_utc.desc()).first()

def is_store_open(row):
    now = datetime.now(pytz.timezone(row['timezone_str']))
    day_name = now.strftime('%A')
    start_time_str = row[f'{day_name}_open']
    end_time_str = row[f'{day_name}_close']
    if start_time_str and end_time_str:
        start_time = datetime.strptime(start_time_str, '%H:%M').time()
        end_time = datetime.strptime(end_time_str, '%H:%M').time()
        store_start_time = datetime.combine(now.date(), start_time, now.tzinfo)
        store_end_time = datetime.combine(now.date(), end_time, now.tzinfo)
        if store_start_time <= now <= store_end_time:
            return True
    return False

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        # Read store_status.csv file into a pandas data frame
        status_df = pd.read_csv('store_status.csv')

        # Convert the data frame into a list of dictionaries
        status_data = status_df.to_dict(orient='records')

        # Use SQLAlchemy to insert the data into the statuses table
        for row in status_data:
            store_id = row.pop('store_id')
            try:
                timestamp_utc = datetime.strptime(row['timestamp_utc'].replace(' UTC', ''), '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                timestamp_utc = datetime.strptime(row['timestamp_utc'].replace(' UTC', ''), '%Y-%m-%d %H:%M:%S')
            status = row['status']
            status_row = Status(store_id=store_id, timestamp_utc=timestamp_utc, status=status)
            db.session.add(status_row)


        # Read business_hours.csv file into a pandas data frame
        hours_df = pd.read_csv('business_hours.csv')

        # Convert the data frame into a list of dictionaries
        hours_data = hours_df.to_dict(orient='records')

        # Use SQLAlchemy to insert the data into the business_hours table
        for row in hours_data:
            store_id = row.pop('store_id')
            day_of_week = row['day']
            start_time_local = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
            end_time_local = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
            hours_row = BusinessHours(store_id=store_id, day_of_week=day_of_week, start_time_local=start_time_local, end_time_local=end_time_local)
            db.session.add(hours_row)

        # Read stores.csv file into a pandas data frame
        stores_df = pd.read_csv('stores.csv')

        # Convert the data frame into a list of dictionaries
        stores_data = stores_df.to_dict(orient='records')

        # Use SQLAlchemy to insert the data into the stores table
        for row in stores_data:
            store_id = row.pop('store_id')
            timezone_str = row['timezone_str']
            store_row = Store(id=store_id, timezone_str=timezone_str)
            db.session.add(store_row)

        # Commit the changes to the database
        db.session.commit()
        
        
    app.run(debug=True,host='0.0.0.0',port=6000)

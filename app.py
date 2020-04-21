import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, join, outerjoin, func
import datetime as dt

from flask import Flask, jsonify

# Connect to DB & reflect tables
Base = automap_base()

# engine, suppose it has two tables 'user' and 'address' set up
engine = create_engine("sqlite:///Resources/hawaii.sqlite", connect_args={'check_same_thread': False})
conn = engine.connect()


# reflect the tables
Base.prepare(engine, reflect=True)

# mapped classes are now created with names by default
# matching that of the table name.
measurement = Base.classes.measurement
station = Base.classes.station

session = Session(engine)

# read the database tables into a pandas dataframe
data_meas = pd.read_sql("SELECT * FROM measurement", conn)
data_stat = pd.read_sql("SELECT * FROM station", conn)

data_meas_sel = pd.read_sql("SELECT date, prcp FROM measurement", conn)

# Flask Set Up
app = Flask(__name__)

# Flask Routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start_end"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query last 12 months of data
    data_max_date = pd.read_sql("SELECT max(date) FROM measurement", conn)
    data_meas['date'] = pd.to_datetime(data_meas['date'])
    data_12mo = (data_meas_sel['date'] > '2016-08-23') & (data_meas_sel['date'] <= '2017-08-23')
    date_12mo_sort = data_12mo.sort_values(by=['date'], ascending=True)

    parcip_totals = []
    for result in rain:
        row = {}
        row["date"] = date_12mo_sort[0]
        row["prcp"] = date_12mo_sort[1]
        rain_totals.append(row)

    return jsonify(parcip_totals)

    session.close()

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB 
    query_stat = session.query(station.name, station.station)
    data_station_active = pd.read_sql("SELECT station,count(*) FROM measurement group by station order by count(*) desc", conn)

    # Query all stations and pull each name
    stat_results = session.query(station.name).all()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(stat_results))

    return jsonify(all_stations)

    session.close()

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB & find the tobs for the most frequent channel
    query_tobs = session.query(measurement.tobs).\
    filter(measurement.date > dt.date(2016,8,23)).\
    filter(measurement.date < dt.date(2017,8,23)).\
    filter(measurement.station == 'USC00519281').all()

    # Convert list of tuples into normal list
    all_tobs = list(np.ravel(query_tobs))

    return jsonify(all_tobs)

    session.close()

@app.route("/api/v1.0/start")
def start():
    # Create our session (link) from Python to the DB & find avg, min, & max temp for specific start of trip onward
    trip_start = dt.date(2017, 7, 23)

    trip_data = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= trip_start).all()

    # Convert list of tuples into normal list
    trip = list(np.ravel(trip_data))

    return jsonify(trip)

    session.close()

@app.route("/api/v1.0/start_end")
def start_end():
    # Create our session (link) from Python to the DB & find avg, min, & max temp for specific trip dates
    trip_start = dt.date(2017, 7, 23)
    trip_end = dt.date(2017, 7, 31)

    trip_data = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)).\
        filter(measurement.date >= trip_start).filter(measurement.date <= trip_end).all()

    # Convert list of tuples into normal list
    trip = list(np.ravel(trip_data))

    return jsonify(trip)

    session.close()

if __name__ == '__main__':
    app.run(debug=True)
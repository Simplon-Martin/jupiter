# coding: utf8
from app import app
from flask import render_template, request, abort, redirect, url_for
import datetime
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from app import models
from joblib import load
from geopy.geocoders import Nominatim
from flask_googlemaps import get_address, get_coordinates
from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map
import time

API_KEY = "AIzaSyDIhNLfbYdsqWlvlCenVPU-qoaAIua11c4"


@app.route('/')
def index():
    date = datetime.datetime.now().strftime("%x %X")
    return render_template( 'index.html', date=date)


@app.route('/dashboard')
def dashboard():
    models.graphique()
    date = datetime.datetime.now().strftime("%x %X")
    return render_template( 'dashboard.html', date=date)


@app.route('/formulaire_predict')
def formulaire_predict():
    date = datetime.datetime.now().strftime("%x %X")
    return render_template( 'formulaire_predict.html', date=date)


@app.route('/predict', methods = ['POST', 'GET'])
def predict():

    address = request.form['address']
    income = request.form['income']
    rooms = request.form['rooms']
    bedrooms = request.form['bedrooms']

    location=get_coordinates(API_KEY,"499 Avenue Janvier Passero Mandelieu France")
    time.sleep(1)
    # model : ["ll", 'longitude', 'latitude', 'rooms_per_household', 'bedrooms_per_household', "median_income"]

    model = load('app/static/model.joblib')
    price = model.predict([[location["lng"]*location["lat"], location["lng"], location["lat"], rooms, bedrooms, income]])[0]

    date = datetime.datetime.now().strftime("%x %X")
    return render_template(
        'predict.html',
        date=date,
        address=address,
        rooms=rooms,
        income=income,
        bedrooms=bedrooms,
        price=price,
        longitude=location.longitude,
        latitude=location.latitude)
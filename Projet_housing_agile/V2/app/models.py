# coding: utf8
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# A modifier 
s = "postgresql+psycopg2://julien:simplon21@localhost/housing"
housing = db.create_engine(s,{})

def graphique():

    data = pd.read_sql_query("SELECT * FROM csv_housing",housing)

    plt.figure(figsize=[5,4])

    sns.set_style("dark")
    sns.set_context("paper")

    scatt = plt.scatter(data["longitude"],data["latitude"],c=data["median_house_value"],cmap=plt.get_cmap("jet"),s=data["population"]/100,alpha=0.4)
    cb = plt.colorbar(scatt)
    cb.set_label('median_house_value')
    plt.title("California Housing Prices")
    plt.xlabel("longitude")
    plt.ylabel("latitude")
    plt.legend("population")

    plt.savefig("app/static/img/fig_carte.png")
    plt.close()


    plt.figure(figsize=[5,4])

    sns.set_style("dark")
    sns.set_context("paper")

    plt.hist(data["median_income"])
    plt.title("Revenu médian des blocs en Californie")
    plt.xlabel("Tranches de revenus en k$")

    plt.savefig("app/static/img/fig_income.png")
    plt.close()

    plt.figure(figsize=[5,4])

    sns.set_style("dark")
    sns.set_context("paper")

    plt.hist(data["median_house_value"])
    plt.title("Prix médian par bloc en Californie")
    plt.xlabel("Tranches de prix")

    plt.savefig("app/static/img/fig_price.png")
    plt.close()

    plt.figure(figsize=[5,4])

    sns.set_style("dark")
    sns.set_context("paper")

    sns.countplot(x=data["ocean_proximity"],data=data["ocean_proximity"])
    plt.title("Proximité à l'océan")
    plt.xlabel("Catérgories ocean_proximity")
    plt.ylabel("Nombre de blocs")

    plt.savefig("app/static/img/fig_oceanproximity.png")
    plt.close()
    
    return None
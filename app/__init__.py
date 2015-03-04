from cassandra.cluster import Cluster
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.from_object('config')

# preload database handle
db = SQLAlchemy(app)

# cassandra database handle
cluster = Cluster()
session = cluster.connect('ooi2')
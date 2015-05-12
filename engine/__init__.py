from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import logging.config


app = Flask(__name__)
app.config.from_object('config')


# Flask configures its own logger deleting
# any previous configuration.
if app.config.get('LOGGING_CONFIG'):
    # Access logger so it is initialized.
    app.logger
    # Then load configuration to overwrite
    logging.config.fileConfig(app.config.get('LOGGING_CONFIG'))

# preload database handle
db = SQLAlchemy(app)
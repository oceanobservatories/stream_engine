from flask import Flask
import logging.config
import preload_database.database

app = Flask(__name__)
app.config.from_object('config')


# Flask configures its own logger deleting
# any previous configuration.
if app.config.get('LOGGING_CONFIG'):
    # Access logger so it is initialized.
    app.logger
    # Then load configuration to overwrite
    # logging.config.fileConfig(app.config.get('LOGGING_CONFIG'))

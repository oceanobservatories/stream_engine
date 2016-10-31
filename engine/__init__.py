import os

from flask import Flask
import logging.config

app = Flask(__name__)
app.config.from_object('config.default')
app.config.from_object('config.local')


# Flask configures its own logger deleting
# any previous configuration.
log_config = os.path.join(os.path.dirname(__file__), '..', app.config.get('LOGGING_CONFIG'))
if log_config and os.path.exists(log_config):
    # Access logger so it is initialized.
    app.logger
    # Then load configuration to overwrite
    logging.config.fileConfig(log_config)

import logging.config
import preload_database.database
from flask import Flask

app = Flask(__name__)
app.config.from_object('config')

# Flask configures its own logger deleting
# any previous configuration.
if app.config.get('LOGGING_CONFIG'):
    # Access logger so it is initialized.
    app.logger
    # Then load configuration to overwrite
    logging.config.fileConfig(app.config.get('LOGGING_CONFIG'))

@app.teardown_appcontext
def shutdown_session(exception=None):
    preload_database.database.Session.remove()

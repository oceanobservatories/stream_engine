from flask import Flask
import logging.config
import preload_database.config
preload_database.config.PRELOAD_DATABASE_MODE = preload_database.config.PreloadDatabaseMode.POPULATED_FILE
from preload_database.database import init_db, db_session
from preload_database.model.preload import *

app = Flask(__name__)
app.config.from_object('config')

# Flask configures its own logger deleting
# any previous configuration.
if app.config.get('LOGGING_CONFIG'):
    # Access logger so it is initialized.
    app.logger
    # Then load configuration to overwrite
    logging.config.fileConfig(app.config.get('LOGGING_CONFIG'))

init_db()

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

#!/usr/bin/env python

from engine.routes import app
from util.cass import get_session, create_execution_pool
import preload_database.database
preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()
get_session()
create_execution_pool()
app.run(debug=True)

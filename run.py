#!/usr/bin/env python

from engine.routes import app
from util.cass import _init
import preload_database.database
preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()

_init()
# The reloader must be disabled so stream engine
# runs on the main thread
app.run(debug=True, use_reloader=False)

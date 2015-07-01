#!/usr/bin/env python

from werkzeug.contrib.profiler import ProfilerMiddleware
from util.cass import get_session, create_execution_pool
import preload_database.database
preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
preload_database.database.open_connection()
get_session()
create_execution_pool()

from engine.routes import app
app.config['PROFILE'] = True
app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
app.run(debug=True)

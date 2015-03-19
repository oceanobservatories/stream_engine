#!/usr/bin/env python

from werkzeug.contrib.profiler import ProfilerMiddleware

from engine.routes import app
app.config['PROFILE'] = True
app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
app.run(debug=True)

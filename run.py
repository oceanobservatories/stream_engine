#!/usr/bin/env python

from engine.routes import app
from util.cass import get_session, create_execution_pool
get_session()
create_execution_pool()
app.run(debug=True)

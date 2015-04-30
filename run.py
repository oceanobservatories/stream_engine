#!/usr/bin/env python

import logging
root = logging.getLogger()
root.setLevel('DEBUG')
root.addHandler(logging.StreamHandler())

from engine.routes import app
app.run(debug=True)
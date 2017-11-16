#!/usr/bin/env python
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from engine.routes import app
from util.cass import _init
from ooi_data.postgres.model import MetadataBase
from preload_database.database import create_engine_from_url

engine = create_engine_from_url(r'postgresql://awips:awips@localhost/metadata')
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
MetadataBase.query = Session.query_property()


_init()
# The reloader must be disabled so stream engine
# runs on the main thread
app.run(debug=True, use_reloader=False)

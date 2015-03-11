#!/usr/bin/env python

from engine import app
app.run(debug=True)

# from gevent.wsgi import WSGIServer
# from engine import routes
#
# http_server = WSGIServer(('', 5000), app)
# http_server.serve_forever()

# from tornado.wsgi import WSGIContainer
# from tornado.httpserver import HTTPServer
# from tornado.ioloop import IOLoop
# import tornado.log
# from engine import app
# import logging
#
#
# def main():
#
#     tornado.log.enable_pretty_logging()
#     server = HTTPServer(WSGIContainer(app))
#     server.bind(5000)
#     server.start(0)
#     logging.getLogger('tornado.application').setLevel(logging.DEBUG)
#     IOLoop.current().start()
#
# if __name__ == '__main__':
#     main()

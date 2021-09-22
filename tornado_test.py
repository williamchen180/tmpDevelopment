# -*- coding: utf-8 -*-
import os
import sys
import time

import tornado.web
import tornado.websocket
import tornado.httpserver
import tornado.ioloop



class MainHandler(tornado.web.RequestHandler):
    def get(self):
        # self.write('Hello, world')
        self.write('<a href="%s">link to story 1</a>' %
                   self.reverse_url("story", "1"))


class StoryHandler(tornado.web.RequestHandler):
    def initialize(self, db):
        self.db = db

    def get(self, story_id):
        self.write("this is story %s" % story_id)

db = dict(a=1, b=2)

def make_app():
    return tornado.web.Application([
        (r'/', MainHandler),
        (r"/story/([0-9]+)", StoryHandler, dict(db=db), "story")
    ])

if __name__ == '__main__':
    app = make_app()
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()


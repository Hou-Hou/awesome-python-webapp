#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# @Author  :  Hou

import logging;
# 通过logging.basicConfig函数对日志的输出格式及方式做相关配置
logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

# index函数负责响应http请求并返回一个html
def index(request):
    return web.Response(body=b'<h1>Awesome</h1>',content_type='text/html')

async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)  # app.router是一个UrlDispatcher对象，它有一个add_route方法
    srv = await loop.create_server(app._make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
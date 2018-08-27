#!/usr/bin/env python3 
# -*- coding:utf-8 -*-

from orm import create_pool, destroy_pool
import asyncio
from models import User
import logging

async def test1(loop):
    await create_pool(loop=loop, host='localhost', port=3306, user='root', password='123456', db='awesome')
    # u = User(name='Test19', email='test19@example.com', passwd='123456',
    #         image='about:blank')
    # await u.save()

    # 测试count rows语句
    rows = await User.countRows()
    logging.info('rows is %s' % rows)
    print('now Table "users" has %s rows' % rows)

    # # 测试delete语句
    # print('------now test delete语句------')
    # users = await User.findAll(orderBy='id')
    # for user in users:
    #     print('delete user: %s' % user.name)
    #     await user.remove()

    # 测试insert into语句
    print('------now test insert into语句------')
    if rows < 6:
        for idx in range(5):
            u = User(
                id=idx,
                name='test%s' % idx,
                email='%s@org.com' % idx,
                passwd='pw%s' % idx,
                image='about:blank'
            )
            row = await User.countRows(where='id = ?', args=[u.id])
            if row == 0:
                print('add user: %s' % u.name)
                await u.save()
            else:
                print('the email is already registered...')

    # 测试select语句
    print('-------now test select语句-------')
    users = await User.findAll(orderBy='created_at')
    for user in users:
        print('name: %s, password: %s, created_at: %s' % (user.name, user.passwd, user.email))

    # 测试update语句
    # user = users[1]
    # user.email = 'guest@orm.com'
    # user.name = 'guest'
    # await user.update()

    # 测试查找指定用户
    # test_user = await User.find(user.id)
    # logging.info('name: %s, email: %s' % (test_user.name, test_user.email))


    await destroy_pool()  # 这里先销毁连接池
    print('test ok')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test1(loop))
    loop.close()
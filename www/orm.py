#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# @Author  :  Hou
# 参考：https://github.com/justoneliu/web_app/blob/master/www/orm.py
import asyncio, logging

import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

async def destroy_pool(): #销毁连接池
    global __pool
    if __pool is not None:
        __pool.close()
        await  __pool.wait_closed()

# 创建连接池 连接池由全局变量__pool存储
'''
create_pool(host='127.0.0.1', port=3306,
            user='root', password='',
            db='mysql', loop=loop)
'''
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop  #需要传递一个事件循环实例，若无特别声明，默认使用asyncio.get_event_loop()
    )

'''
SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换
如果传入size参数，就通过fetchmany()获取最多指定数量的记录，否则，通过fetchall()获取所有记录
'''
# SELECT语句
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:  #从连接池中获取一个连接，使用完后自动释放
        async with conn.cursor(aiomysql.DictCursor) as cur:  #创建一个游标，返回由dict组成的list
            await cur.execute(sql.replace('?', '%s'), args or ())
            # 执行SQL，mysql的占位符是%s，和python一样，为了coding的便利，
            # 先用SQL的占位符？写SQL语句，最后执行时在转换过来
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

# 定义一个通用的execute()函数,执行Insert, Update, Delete语句
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            '''
            class DictCursor: 
            A cursor which returns results as a dictionary. All methods and arguments same as Cursor
          '''
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

# 制作参数字符串
def create_args_string(num):
    L = []
    for n in range(num):  # SQL的占位符是？，num是多少就插入多少个占位符
        L.append('?')
    return ', '.join(L)  # 将L拼接成字符串返回，例如num=3时："?, ?, ?"

'''
'xxx'.join(L)将一些小的字符串合并成一个大的字符串，用xxx连接
L = ['a', 'b', 'c', 'd', 'e']
''.join(L)
print(L)   结果：'abcde'
'''
# 定义数据类型的基类
class Field(object):

    def __init__(self, name, column_type, primary_key, default):  # 可传入参数对应列名、数据类型、主键、默认值
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# 定义元类：Model只是一个基类，如何将具体的子类如User的映射信息读取出来呢？
# 答案就是通过metaclass：ModelMetaclass
'''
任何继承自Model的类（比如User），会自动通过ModelMetaclass扫描映射关系，
并存储到自身的类属性如__table__、__mappings__中
'''
class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):  # 用metaclass=ModelMetaclass创建类时，通过这个方法生成类
        if name=='Model':
            return type.__new__(cls, name, bases, attrs) #当前准备创建的类的对象、类的名字model、类继承的父类集合、类的方法集合
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# 定义所有ORM映射的基类Model：Model从dict继承，所以具备所有dict的功能
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod   # 添加类方法，对应查表，默认查整个表，可通过where limit设置查找条件
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)  # 构造更新后的select语句，并执行，返回属性值[{},{},{}]
        return [cls(**r) for r in rs]  # 返回一个列表,每个元素为每行记录作为一个dict传入当前类的对象的返回值

    @classmethod  # 添加类方法，查找特定列，可通过where设置条件
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']  # 根据别名key取值


    @classmethod
    async def countRows(cls, selectField='*', where=None, args=None):
        ' find number by select and where. '
        sql = ['select count(%s) _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where %s' % where)
        resultset = await select(' '.join(sql), args, 1)   # size = 1
        if not resultset:
            return 0
        return resultset[0].get('_num_', 0)

    @classmethod  # 类方法，根据primary key查询一条记录
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)
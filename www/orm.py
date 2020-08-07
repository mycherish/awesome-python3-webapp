# -*- coding: utf-8 -*-

__author__ = 'xuweidong'

import asyncio, logging
import aiomysql

def log(sql, args=()):
    logging.info('SQL: %s' % sql)

' 创建全局的连接池 '
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    # kw.get()的方式直接定义，kw['']的方式需要传入相应的属性
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['root'],
        password = kw['123456'],
        db = kw['python_db'],
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

' 执行Select
async def select(sql, args, size=None):
    log(sql, args)
    global __loop
    async with __loop.get() as conn:
        # aiomysql.DictCursor将结果作为字典返回
        async with conn.cursor(aiomysql, DictCursor) as cur:
            # 执行语句，第一个参数传入sql语句并将语句中的?替换为%s，第二个语句传入参数
            await cur.execute(sql.replace('?', '%s'), args or ())
            # 如果size有值根据值获取行数，没有值时默认为None查询所有数据
            if size:
                # 指定一次要获取的行数
                rs = await cur.fetchmany(size)
            else:
                # 返回查询结果集的所有行（查到的所有数据）
                rs = await cur.fetchall()
        logging.info('rows return: %s' %s len(rs))
        return rs

' 执行Insert, Update, Delete
asycn def execute(sql, args, autocommit=True):
    log(sql)
    await with __pool.get() as conn:
    # 执行改变数据的语句时判断是否自动提交，not True相当于False
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql, DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                
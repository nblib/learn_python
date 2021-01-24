"""
使用 SA的sql语法操作数据库
"""

"""
检查版本
"""
import sqlalchemy

sqlalchemy.__version__

"""
连接数据库
设置编码
echo输出执行日志
"""
from sqlalchemy import create_engine

# 创建 访问数据库的接口, 此时还没有真正连接
# engine自带连接池
engine = create_engine('mysql+pymysql://hewe:hewe0824@127.0.0.1:3306/FINANCE?charset=utf8', encoding='utf8', echo=True)

"""
创建表
连接mysql时,String代表varchar类型,必须指定长度,不然报错
"""
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

# 定义一个metadata,代表一个目录
metadata = MetaData()
# 创建users表
users = Table('users', metadata,
              Column('id', Integer, primary_key=True, comment='id'),  # 主键
              Column('name', String(10), nullable=False, comment='名称'),
              Column('fullname', String(20), comment='全名')
              )
# addresses 表, 关联users
addresses = Table('addresses', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('email_address', String(100), nullable=False),
                  extend_existing=True  # 如果执行后发现有内容要改,设为True表示可以重新在metatable中定义表
                  )
# 创建
metadata.create_all(engine)
# metadata.create_all(engine, tables=[users]) # 只创建 tables中的表,其他不创建

"""
插入数据
多行模式下mysql不支持查看预编译语句
插入多行时,每行的key必须相同
"""
# 生成insert语句,包含所有列
ins = users.insert()
# 查看生成的SQL语句
str(ins)
# 通过绑定engine,可以看到不同数据库的不同预编译insert语句的格式
ins.bind = engine
str(ins)
# 只根据指定的列生成insert语句
ins = users.insert().values(name='jack', fullname='Jack Jones')
str(ins)
# 编译语句并查看占位参数
ins.compile().params

# 插入多行
# ins = users.insert().values([{"name":"jack", "fullname":"Jack Jones"},{"name":"jack", "fullname":"Jack Jones"}])
# 多行模式下mysql不支持查看预编译语句
# str(ins)
"""
执行语句
"""
# 获取一个连接
conn = engine.connect()
# 执行
result = conn.execute(ins)
# result 代表 cursor, 通过它可以看到返回内容,insert的话,可以看到插入的主键值
result.inserted_primary_key
# 执行的时候指定参数
conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')
# 插入多行
# 每行的key必须相同
conn.execute(addresses.insert(), [
    {'user_id': 1, 'email_address': 'jack@yahoo.com'},
    {'user_id': 1, 'email_address': 'jack@msn.com'},
    {'user_id': 2, 'email_address': 'www@www.org'},
    {'user_id': 2, 'email_address': 'wendy@aol.com'},
])

"""
查询操作
"""
from sqlalchemy import select

s = select([users])
result = conn.execute(s)
# 获取结果
# 查看返回数量
result.rowcount
# 遍历
for row in result:
    print(row)
# 获取一行
result = conn.execute(s)
result.fetchone()
# 获取所有
result = conn.execute(s)
result.fetchall()
# 通过列名获取
for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])
# 关闭result: 默认情况下,读取完所有结果便会自动关闭, 如果没有读取完,需要手动关闭
result.close()
# 获取指定列
s = select([users.c.name, users.c.fullname])
for row in conn.execute(s):
    print(row)

"""
查询的where条件
"""

# 查询两个表,默认情况下返回两个表内容的笛卡尔积
s = select([users, addresses])
for row in conn.execute(s):
    print(row)
# 正确情况,应该添加查询条件
s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)

# users.c.id == addresses.c.user_id 并不是True或False,而是返回一个表达式,因为重写了__eq__方法
str(users.c.id == addresses.c.user_id)
# 常用条件表达式
print(users.c.id != 7)  # id !=7
print(users.c.name == None)  # name is NULL
print('fred' > users.c.name)  # name < 'fred
print(users.c.id + addresses.c.id)  # u.id + a.id
print(users.c.name + users.c.fullname)  # 字符串类型默认为 users.name || users.fullname
print((users.c.name + users.c.fullname).compile(bind=engine))  # 使用 mysql时,为concat方法: concat(users.name, users.fullname)
print(users.c.name.op('<')('foo'))  # 如果特殊操作,比如自定义函数,可以使用这个方法,生成: users.name < :name_1
print(users.c.name.op('ooo')('foo'))  # 如果特殊操作,比如自定义函数, 可以使用这个方法,生成: users.name ooo :name_1
# 显式定义自定义操作的返回类型
# from sqlalchemy import type_coerce
# expr = type_coerce(somecolumn.op('-%>')('foo'), MySpecialType())
# stmt = select([expr])
# 定义自定义操作返回类型为bool
# somecolumn.bool_op('-->')('some value')

"""
where 条件使用连接符
目前没有实现 order by,group by,having操作
"""
from sqlalchemy import and_, or_, not_

# and, or, not 连接符
print(
    and_(
        users.c.name.like('j%'),
        users.c.id == addresses.c.user_id,
        or_(
            addresses.c.email_address == 'wendy@aol.com',
            addresses.c.email_address == 'jack@yahoo.com',
        ),
        not_(users.c.id > 5)
    )
)
# 使用位运算符表示and,or,not
print(users.c.name.like('j%') & (users.c.id == addresses.c.user_id) &
      (
              (addresses.c.email_address == 'wendy@aol.com') |
              (addresses.c.email_address == 'jack@yahoo.com')
      )
      & ~(users.c.id > 5)
      )
# as,between的使用
s = select([(users.c.fullname +
             ", " + addresses.c.email_address).
           label('title')]). \
    where(
    and_(
        users.c.id == addresses.c.user_id,
        users.c.name.between('m', 'z'),
        or_(
            addresses.c.email_address.like('%@aol.com'),
            addresses.c.email_address.like('%@msn.com')
        )
    )
)
conn.execute(s).fetchall()

# 多个where相当于and
# select().where(...).where(...)

# sdbmigrationtool
SequoiaDB 数据库集合间数据迁移工具，支持结构化、半结构化和 Lob 对象的迁移，提供多线程、限速、校验、修复功能。
当前版本支持功能如下：
- 提供多线程并发功能
- 支持输出迁移速率的统计信息
- 结构化、半结构化数据只支持数据迁移，并且自动忽略oid已存在的记录
- Lob 对象支持数据迁移、数据校验、数据修复功能

> 注：
> 1. 数据校验功能是比对数据迁移前后的 Lob 对象大小以及其 md5值是否相同，若大小或者md5值不相同时，则会在脚本所在的当前目录生成以集合名+“_年月日时分秒.dat”格式命名的数据文件，记录校验不通过的 Lob 对象的 oid 值 和 Flag 标识。
> 2. 数据修复功能是指定数据校验操作生成的数据文件，重新迁移这部分的数据。

# 程序打包
1. 在终端执行 `mvn clean package`，会在项目所在路径的 target 目录中生成 sdbmigrationtool目录以及程序的压缩包 sdbmigrationtool.tar.gz
2. 压缩包解压后的目录结构如下
```
sdbmigrationtool
    -->bin
    -->conf
    -->lib
    README.md
```
> - bin : 存放工具的启停脚本
> - conf : 存放工具日志的配置文件
> - lib : 存放工具的jar运行包和依赖库
> - README.md : 工具的介绍与使用说明

# 程序使用

1. 工具执行的命令格式如下：
```
sh sdbmigrationtool.sh [options]
```
2. 加密工具使用, 根据提示输入待加密密码
```
sh encrypt.sh
```

# 程序参数说明

- `-o,--operation <arg>`, 指定操作类型，支持迁移(migration),校验(validate),修复(repair)
- `-t,--type <arg>`, 指定数据类型，支持 lob/data, lob 指Lob对象, data 指结构化、半结构化数据
- `-h,--hosts <arg>`, 指定源端 SequoiaDB 数据库协调节点地址，多个地址以","分隔,默认值为"localhost:11810"
- `-u,--user <arg>`, 指定源端 SequoiaDB 数据库连接用户
- `-p,--password <arg>`, 指定源端 SequoiaDB 数据库连接密码
- `-c,--collection <arg>`, 指定源端 SequoiaDB 数据库的集合 Full 名, 例如: "cs.cl"
- `-dsth,--dstcollection <arg>`, 指定目标端 SequoiaDB 数据库协调节点地址，多个地址以","分隔,默认值为"localhost:11810"
- `-dstu,--dstuser <arg>`, 指定目标端 SequoiaDB 数据库连接用户
- `-dstp,--dstpassword <arg>`, 指定目标端 SequoiaDB 数据库连接密码
- `-dstc,--dstcollection <arg>`, 指定目标端 SequoiaDB 数据库的集合 Full 名, 例如: "dstcs.dstcl"
- `-crd,--coord <arg>`, 自动查找目标端 SequoiaDB 数据库的协调节点，默认值为：true
- `-e,--encrypt <arg>`, 指定密码加密类型，支持 text/sm2, text 表示明文，sm2 表示 sm2 加密, 默认值为: text
- `-i,--interval <arg>, 指定输出执行统计信息周期，单位为秒，默认为 0 ,表示不输出`
- `-j,--jobs <arg>`, 指定线程数量，默认值为 1
- `-f,--file <arg>`, 指定修复操作的数据文件，只在修复操作时生效
- `-r,--rate <arg>`, 指定最大传输流量，单位为 MB/s, 只在数据类型为 Lob 对象时生效，默认值为0
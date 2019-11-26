# Redis Client C++
## 一、项目介绍
本项目是基于开源hiredis操作，使用C++语言，对于Redis常用指令进行封装，以便在项目中直接调用RedisClient成员方法即可实现对于Redis数据库操作。

## 二、基本使用
基本调用方式已经写到了example文件夹中，可在项目路径下执行make.sh进行编译生成可执行文件，可执行文件保存到bin文件夹下。

执行方法及参数均在下面列出：

| 执行方式 | 含义 |
| ---- | ---- |
| ./make.sh string | 生成数据结构string example 可执行文件 |
| ./make.sh hash | 生成数据结构hash example 可执行文件 |
| ./make.sh list | 生成数据结构list example 可执行文件 |
| ./make.sh set | 生成数据结构set example 可执行文件 |
| ./make.sh zset | 生成数据结构zset example 可执行文件 |
| ./make.sh clean | 清除所有.o文件 |


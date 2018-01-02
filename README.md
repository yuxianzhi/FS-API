## File System Python API

Unified interface for file system, An wrapper to compat some common file system.

We can only support hdfs and file protocol at present.

### Standard

Standard to use: <protocol>://<url><absolute_path>

```plantuml
@startuml

actor User
User --> [ file_system ]

note right of [ file_system ]
    open()
    close()
    read()
    readline()
    readlines()
    write()
    writeline()
    writelines()
    copy()
    exists()
    remove()
    listdir()
end note

package file {
     component 本地文件系统函数{
         control python的os模块
    }
}
package hdfs {
     component hdfs文件系统函数{
         control python的hdfs模块
    }
}
[ file_system ] -> file
[ file_system ] -> hdfs
```


#### local file system

* standard: file://<absolute_path>

* example:  file:///home/ts/test.txt


#### hdfs

* standard: hdfs://[<ip|hostname>:<port>]<absolute_path>

* example:  hdfs://10.147.20.122:50070/user/ts/test.txt

#### How to use

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

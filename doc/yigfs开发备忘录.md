# 待完成：
## server端：
1. meta server需要配置动态log
2. 已经删除的block未写入gc
3. list目录第一次比较慢的问题
4. 新建一个文件，返回给client一个未满的segment
## client端：
1. 枚举目录下的segment文件
2. data写的时候传地址
3. client中file的name长度最长限制为255个字节
4. client中获取目录的可用空间及使用量判断，可以使用statfs
5. client加一个io的queue，目前用的channel来代替的，但是channel中有个数限制
6. 多个连续的write操作可以合并为一个操作


# yigfs测试说明
|            测试场景          |          说明         |      备注      |
| --------------------------- | --------------------- |----------------|      
| 两台机器同时创建一个同名文件  | 两台机器挂载同一个bucket|                 |

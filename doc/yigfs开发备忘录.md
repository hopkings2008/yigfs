# 待完成：
## server端：
1. meta server需要配置动态log
2. 已经删除的block未写入gc
3. list目录第一次比较慢的问题
4. 新建一个文件，返回给client一个未满的segment（需要返回增长速度最慢的segment，目前返回的是剩余空间最大的segment）
5. heartBeat更新server状态
6. 更新文件segments信息接口需要将其做成一个大的事务

## client端：
1. 枚举目录下的segment文件
2. data写的时候传地址
3. client中file的name长度最长限制为255个字节
4. client中获取目录的可用空间及使用量判断，可以使用statfs
5. client加一个io的queue，目前用的channel来代替的，但是channel中有个数限制
6. 多个连续的write操作可以合并为一个操作
7. 优化read接口中，对于offset+size在segment中的定位，目前是O(n)的时间，可以优化为O(log(n))的时间(已完成)
8. add request identifier and record it in the log
9. 减少segments及blocks的拷贝次数(已完成)
10. 记录被修改的block，对于文件的修改和写入，只传输被修改的blocks
11. oflags关联到filehandle中进行后续的判断


# yigfs测试说明
|            测试场景          |          说明         |      备注      |
| --------------------------- | --------------------- |----------------|      
| 两台机器同时创建一个同名文件  | 两台机器挂载同一个bucket|                 |

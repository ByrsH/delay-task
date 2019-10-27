# delay-task
基于 redis 实现的延迟任务框架

注意：还未经过大规模应用，谨慎用于生产环境

## 设计思想

利用 Redis 有序集合（sorted set）的特性：每个元素都会关联一个 double 类型的分数，redis 会通过这个分数来为集合中的成员进行从小到大的排序。从而实现的延迟任务处理。框架使用两个线程和两个有序集合来实现，两个有序集合：一个用于存储还未到时间的任务，一个用于存储正在执行的任务。两个线程：一个是工作线程，不断的从待执行任务集合中取出到时要执行的任务，存入正在执行的集合，并且返回客户端用于执行；另一个线程时检查线程，用于处理正在执行集合中超时未确认的任务，把这些任务重新放入待执行集合。

![在这里插入图片描述](https://img-blog.csdnimg.cn/20191027175249803.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3UwMTA2NTcwOTQ=,size_16,color_FFFFFF,t_70)

## 用例

参考测试用例：https://github.com/ByrsH/delay-task/blob/master/src/test/java/com/byrsh/delaytask/DelayTaskTest.java

## 未来实现功能

支持 redis 集群部署模式



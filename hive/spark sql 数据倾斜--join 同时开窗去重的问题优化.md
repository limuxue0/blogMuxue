# spark sql 数据倾斜--join 同时开窗去重的问题优化

背景：

> 需求：在一张查询日志表中，有百亿数据，需要join上维表，再根据几个字段进行去重

## 结论

开窗去重和join**一定要分步进行**，按照需求先做join再开窗，或者去重完成后在进行join。

## 1.  原方案：join步骤时，同时开窗去重

dwd_tmp1 中存在百亿用户查询日志数据

```sql
insert into table dws_tmp1 partitions(week='xxxx')
select 
c1,
c2,
c3,
c4,
other..
from(
    select t1.*,
    -- 开窗去重
    row_number row_number() over(partition by c1,c2,c3,d1.c5 order by c1,c3,d1.c5,c2) as rank 
   	from dwd_tmp1 t1
    -- join 维表
    left join dim_d1 d1 on t1.id=d1.id
)t2
where t2.rank =1
```

### 数据倾斜

数据量超百亿，资源给到200 * 2c * 20G，执行引擎为spark。由于环境涉及公司机密，不便放图，只谈生产调优经验。

在执行上面SQL代码，观察调度spark任务信息，总共划分为5个stage【0-4】，stage2 会一直卡顿，shuffle容量膨胀到数百G，点进stage2中，存在几个task 读和写的容量超20G，其他一般都在几百M。经过多次测试stage2卡顿时间在2H左右。

## 2. 优化

### 2.1 参数调优

distribute by ：只对最后写入数据块的数据分布起效果，对中间的shuffle分区数量无用。

spark.sql.shuffle.partitions：由于提交执行的代码是spark sql，所以设置spark.sql.shuffle.partitions数量；经过分析spark SQL的代码执行计划，该参数配置能有效改变代码执行过程中各个stage的shuffle分区数量。多测测试【600-4000】范围之间，效果并不理想，stage2还是存在卡顿，稍微好一点1.7h。

注意：**spark.default.parallelism只有在处理RDD时有效**；

**官网建议: spark.sql.shuffle.partitions设置为当前spark job的总core数量的2~3倍；**

### 2.2 SQL优化

最终回归到SQL问题分析上，将上面SQL拆分，一步一步进行测试执行，发现都执行很快，所以问题直接定位到开窗去重和join同步进行的位置。

最终解决：**用子查询进行join之后，再开窗去重**（由于开窗去重中有一个字段是需要关联维表获取），效果明显。相同的资源配置参数下，10m完成运行，最后调整合适资源。



# SparkApp

记录 PySpark 的一些应用案例

## 1.SparkApp by PySpark

### 1.1 sparkcore

/sparkcore 中实现了RDD的 join 和自定义累加器

### 1.2 ecommerce

/ecommerce 中实现了电商平台的分析案例

数据集：电商网站的用户行为数据

主要包含用户的 4 种行为： 搜索，点击，下单，支付。
数据规则如下：

* 数据文件中每行数据采用下划线(_)分隔数据
* 每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
* 如果搜索关键字为 null,表示数据不是搜索数据
* 如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
* 针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个， id 之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
* 支付行为和下单行为类似

数据集 [user_visit_action.txt](data/user_visit_action.txt) 的 schema 如下表

|  编号   | 字段名称  | 字段类型  |字段含义  |
|  ----  | ----  | ----  |----  |
| 0 | date | String | 用户点击行为的日期 |
| 1 | user_id | Long | 用户的 ID| 
| 2 | session_id | String | Session 的 ID| 
| 3 | page_id | Long | 某个页面的 ID| 
| 4 | action_time|  String|  动作的时间点| 
| 5 | search_keyword|  String|  用户搜索的关键词| 
| 6 | click_category_id | Long|  某一个商品品类的 ID| 
| 7 | click_product_id|  Long | 某一个商品的 ID| 
| 8 | order_category_ids|  String|  一次订单中所有品类的 ID 集合| 
| 9 |  order_product_ids|  String|  一次订单中所有商品的 ID 集合| 
| 10|  pay_category_ids|  String|  一次支付中所有品类的 ID 集合| 
| 11| pay_product_ids|  String|  一次支付中所有商品的 ID 集合| 
| 12|  city_id|  Long|  城市 id| 



#### 1.2.1 Top10 热门品类

代码位置: [ecommerce/HotCategoryTop10Analysis](ecommerce/HotCategoryTop10Analysis.py)

品类是指产品的分类，大型电商网站品类分多级，本项目中品类只有一级。
我们按照每个品类的点击、下单、支付的量来统计热门品类。
先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下
单数；下单数再相同，就比较支付数。

#### 1.2.2 Top10 热门品类中每个品类的 Top10 活跃 Session 统计

代码位置: [ecommerce/HotCategoryTop10SessionAnalysis](ecommerce/HotCategoryTop10SessionAnalysis.py)

在需求一的基础上，增加每个品类用户 session 的点击统计

#### 1.2.3 页面单跳转换率统计

代码位置: [ecommerce/PageflowAnalysis](ecommerce/PageflowAnalysis.py)

页面单跳转换率：用户在一次 Session 过程中
访问的页面路径 3,5,7,9,10,21，那么页面 3 跳到页面 5 叫一次单跳， 7-9 也叫一次单跳，
那么单跳转化率就是要统计页面点击的概率。

比如：计算 3->5 的单跳转化率，先获取符合条件的 Session 对于页面 3 的访问次数（PV）
为 A，然后获取符合条件的 Session 中访问了页面 3 又紧接着访问了页面 5 的次数为 B，
那么 B/A 就是 3-5 的页面单跳转化率。

## 2.Notes

1. 使用 PySpark 要注意 Spark 和 python 的版本兼容问题

本项目测试环境为:

java | scala | spark | python | 
-----| ------|-------| ------| 
1.8| 2.11 | 2.4  | 3.7 |  






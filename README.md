# tispark
A cannot-be-more-trivial implementation of tidb-spark-connector.

<<<<<<< HEAD

=======
>>>>>>> 261a30a9ccd7c9969cd06666a150e9e9dd860667
Uses as below
```
./spark-shell --jars /where-ever-it-is/tispark-0.1.0-SNAPSHOT-jar-with-dependencies.jar

import com.pingcap.tispark._
import org.apache.spark.sql.TiContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
<<<<<<< HEAD
val ti = new TiContext(sqlContext.sparkSession, List("127.0.0.1:" + 2379))


val df = ti.tidbTable("global_temp", "item")
df.createGlobalTempView("item")

spark.sql("select sum(discount) as avg_disc from global_temp.item").show

spark.sql("select count(*) as avg_disc from global_temp.item").show

=======
val ti = new TiContext(sqlContext.sparkSession)


val df = ti.tidbTable(List("127.0.0.1:" + 2379), "test", "t2")
df.createGlobalTempView("people")
spark.sql("select c2, c3, avg(c1 + 1) + 1 as r1, count(*) as r2 from global_temp.people where c1 > 0 group by c2, c3").show
```
## Test
>>>>>>> 261a30a9ccd7c9969cd06666a150e9e9dd860667

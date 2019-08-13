import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class AccessLogAgg {
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("IpAgg")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  val file = sc.textFile("dataset/access_log_sample.txt")
  @Test
  //总访问量
  def PV={

    println(file.count())
    sc.stop()
  }
  @Test
  //点击流日志数据————uv总量，PV去重
  def UV={
    val distinctIps = file.map(_.split(" ")(0)).distinct()
    println(distinctIps.count())
    sc.stop()
  }
  @Test
  def ipAgg()={
    val value = file.map(item =>( item.split(" ")(0),1))
    //判断ip地址不为空
    val ipvalue = value.filter(item=>StringUtils.isNotBlank(item._1))
    val ipagg = ipvalue.reduceByKey((curr,agg)=>curr+agg)
    //对结果进行排序，按词频降序排序
    val result = ipagg.sortBy(item=>item._2,false)
    //取前十条记录
    val top10 = result.take(10)
    top10.foreach(item=>println(item))
    sc.stop()
  }


}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.lang3.StringUtils
import org.junit.Test
class StagePractice {
  //创建SparkContext
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("stagePractice")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  @Test
  def pmProcess():Unit={
    //读取文件
    val source: RDD[String] = sc.textFile("dataset/BeijingPM20100101_20151231_noheader.csv")
    //抽取数据 年, 月, PM, 返回结果: ((年, 月), PM)
/*    val result=source.map(item=>((item.split(",")(1),item.split(",")(2)),item.split(",")(6)))
    //    2. 清洗, 过滤掉空的字符串, 过滤掉 NA
      .filter(item=>StringUtils.isNotEmpty(item._2) && ! item._2.equalsIgnoreCase("NA"))
        .map(item=>(item._1,item._2.toInt))
    //    3. 聚合
      .reduceByKey((curr,agg)=>curr+agg)
    //    4. 排序
      .sortBy(item=>item._2,ascending = false)

    result.take(10).foreach(println(_))*/

   val rdd1=source.map(item=>((item.split(",")(1)+"-"+item.split(",")(2)),item.split(",")(6)))
    //    2. 清洗, 过滤掉空的字符串, 过滤掉 NA
   val rdd2=rdd1.filter(item=>StringUtils.isNotEmpty(item._2) && ! item._2.equalsIgnoreCase("NA"))
   val rdd3=rdd2.map(item=>(item._1,item._2.toInt))



   val rdd4 = rdd3.groupByKey()
    rdd4.foreach(println(_))
   val result = rdd4.map(item=>(item._1,(item._2.sum/item._2.size)))
   //    3. 聚合
  // val rdd4=rdd3 .reduceByKey((curr,agg)=>curr+agg)
   //    4. 排序
 //  val result=rdd4 .sortBy(item=>item._2,ascending = false)

   //result.take(10).foreach(println(_))
    println(rdd1.partitions.size)
  }
}

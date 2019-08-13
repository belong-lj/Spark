import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class CacheOp {
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("CacheOp")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  @Test
  def cacheDemo():Unit={
    //读取文件
    val source: RDD[String] = sc.textFile("dataset/access_log_sample.txt")
    //抽取数据
    var aggRDD: RDD[(String, Int)] = source.map(item => (item.split(" ")(0), 1))
      //清洗数据
      .filter(item => StringUtils.isNotEmpty(item._1))
      .reduceByKey((curr, agg) => curr + agg)
    //cache 方法进行缓存,cache(): this.type = persist()
    //aggRDD= aggRDD.cache()
    //将aggRDD这个结果放入缓存中，减少shuffle操作

    //persist 方法,能够指定缓存的级别
    aggRDD = aggRDD.persist(StorageLevel.MEMORY_ONLY)

    val lessIp: (String, Int)=aggRDD.sortBy(item=>item._2,ascending = true).first()
    val moreIp: (String, Int) = aggRDD.sortBy(item=>item._2,ascending = false).first()
    println(lessIp)
    println(moreIp)
  }


  @Test
  def checkpoint(): Unit = {
    // 设置保存 checkpoint 的目录, 也可以设置为 HDFS 上的目录
    sc.setCheckpointDir("checkpoint")
    // RDD 的处理部分
    val source = sc.textFile("dataset/access_log_sample.txt")
    val countRDD = source.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg )

    // checkpoint
    // aggRDD = aggRDD.cache
    // 不准确的说, Checkpoint 是一个 Action 操作, 也就是说
    // 如果调用 checkpoint, 则会重新计算一下 RDD, 然后把结果存在 HDFS 或者本地目录中
    // 所以, 应该在 Checkpoint 之前, 进行一次 Cache
    aggRDD = aggRDD.cache()

    aggRDD.checkpoint()

    // 两个 RDD 的 Action 操作
    // 每一个 Action 都会完整运行一下 RDD 的整个血统
    val lessIp = aggRDD.sortBy(item => item._2, ascending = true).first()
    val moreIp = aggRDD.sortBy(item => item._2, ascending = false).first()

    println((lessIp, moreIp))
  }
}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
object WordCounts {
  def main(args: Array[String]): Unit = {
    //创建Spark Context
    val conf = new SparkConf().setMaster("local[6]").setAppName("word_count")
 //   conf.set("spark.testing.memory", "2147480000")
    val sc:SparkContext = new SparkContext(conf)


    //读取文件并计算词频
    val source: RDD[String] = sc.textFile("hdfs://node01:8020/data/wordcount.txt", 2)
   // val rdd1 = sc.textFile("dataset/wordcount.txt")
    val words: RDD[String]  = source.flatMap(_.split(" "))
   // val words: RDD[String]  = rdd1.flatMap(_.split(" "))
    val wordsTuple: RDD[(String, Int)]=words.map(word=>(word,1))
    val wordsCount: RDD[(String, Int)] =wordsTuple.reduceByKey(_+_)

    //查看执行结果
    val result = wordsCount.collect
    result.foreach(i => println(i))
  }
/*
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("spark_context")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  //创建入口,控制上下文
  //SparkContext身为大入口API, 应该能够创建 RDD, 并且设置参数, 设置Jar包...
/*  @Test
  def sparkContext()={
    val conf=new SparkConf().setMaster("local[6]").setAppName("spark_context")
    val sc = new SparkContext(conf)
    // 2. 关闭 SparkContext, 释放集群资源
  }*/

  // 从本地集合创建
  @Test
  def rddCreationLocal()={
    val seq = Seq(1,2,3)
    val rdd1 = sc.parallelize(seq,2)
    val rdd2 = sc.makeRDD(seq,2)
  }

/*  // 从文件创建
  @Test
  def rddCreationFiles()={
    sc.textFile("hdfs://node01:8020/data/wordcount.txt")
    //textFile 传入的是什么
    //    * 传入的是一个 路径, 读取路径
    //    * hdfs://  file://   /.../...(这种方式分为在集群中执行还是在本地执行, 如果在集群中, 读的是hdfs, 本地读的是文件系统)
    // 2. 是否支持分区?
    //    * 假如传入的path是 hdfs:///....
    //    * 分区是由HDFS中文件的block决定的
    // 3. 支持什么平台
    //    * 支持aws和阿里云
  }*/

  // 从RDD衍生
  @Test
  def rddCreateFromRDD()={
    val rdd1 = sc.parallelize(Seq(1,2,3))
    // 通过在rdd上执行算子操作, 会生成新的 rdd
    // 原地计算
    // str.substr 返回新的字符串, 非原地计算
    // 和字符串中的方式很像, 字符串是可变的吗?
    // RDD可变吗?不可变
    val rdd2 = rdd1.map(item=>item)
    rdd2.foreach(item=>println(item))
  }

  @Test
  def mapTest()={
    //创建rdd
    val rdd1 = sc.parallelize(Seq(1,2,3))
    // 2. 执行 map 操作
    val rdd2 = rdd1.map( item => item * 10 )
    // 3. 得到结果
    val result = rdd2.collect()
    result.foreach(item =>println(item))
  }*/
}

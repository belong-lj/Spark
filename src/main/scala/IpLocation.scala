import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
  /*
1、加载城市ip段信息，获取ip起始数字和结束数字，经度，维度
2、加载日志数据，获取ip信息，然后转换为数字，和ip段比较
3、比较的时候采用二分法查找，找到对应的经度和维度
4、然后对经度和维度做单词计数
*/
  val conf: SparkConf = new SparkConf().setMaster("local[6]").setAppName("iplocation")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)

  //将ip地址转化为Long ,与范围值作比较
  def ipToLong(ip: String): Long = {
    //这里有固定的算法
    val ips = ip.split("\\.")
    //给ipnum一个初始值
    var ipNum: Long = 0L
    for (i <- ips) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //利用二分法查询，查询到long类型的数字在数组的下标
  def binarySearch(ipNum: Long, broadcastValue: Array[(String, String, String, String, String)]): Int = {
    //开始下标
    var start = 0
    //结束下标
    var end = broadcastValue.length - 1
    while (start <= end) {
      var middle = (start + end) / 2
      if (ipNum >= broadcastValue(middle)._1.toLong && ipNum <= broadcastValue(middle)._2.toLong) {
        return middle
      }
      if (ipNum > broadcastValue(middle)._2.toLong) {
        start = middle
      }
      if (ipNum < broadcastValue(middle)._1.toLong) {
        end = middle
      }
    }
    //如果没有找到，则返回-1
    -1
  }

  def datatomysql(iterator: Iterator[((String, String), Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO iplocation (longitude,latitude,total_count) VALUES (?,?,?)"

    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark", "root", "123456")
    iterator.foreach(line => {
      ps = conn.prepareStatement(sql)
      ps.setString(1, line._1._1)
      ps.setString(2, line._1._2)
      ps.setInt(3, line._2)
      ps.executeUpdate()
    })

    if (ps != null)
      ps.close()
    if (conn != null)
      conn.close()

  }


//  def datatomysql(iter:Iterator[((String,String), Int)]) = {
//    //定义数据库连接
//    var conn:Connection=null
//    //预编译sql语句
//    var ps:PreparedStatement=null
//    //编写sql语句,?表示占位符
//    val sql="insert into iplocation(longitude,latitude,total_count) values(?,?,?)"
//    //获得数据库连接
//    conn=DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/spark","root","123456")
//    ps =conn.prepareStatement(sql)
//    iter.foreach(line=>{
//      ps.setString(1,line._1._1)
//      ps.setString(2,line._1._2)
//      ps.setInt(3,line._2)
//      //执行语句
//      ps.execute()
//
//    })
//  }

  def main(args: Array[String]): Unit = {

    /*读取基站数据*/
    val data: RDD[String] =sc.textFile("dataset/ip.txt")
    /*对基站数据进行切分 ，获取需要的字段 （ipStart,ipEnd,城市位置，经度，纬度）*/
    val jizhanData: Array[(String, String, String, String, String)] = data.map(_.split("\\|"))
      .map(x => (x(2), x(3), x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8), x(13), x(14)))
      .collect()
    //广播变量，一个只读的数据区，所有的task都能读到的地方
    //将城市ip信息广播到每一个worker节点
    val cityIpBroadcast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanData)

    /*获取日志数据,获取所有ip地址*/
    val destData =sc.textFile("dataset/20090121000132.394251.txt")
      .map(_.split("\\|")(1))

    //5、遍历所有IP地址，去广播变量中进行匹配，获取对应的经度和维度s
//    destData.foreach(println(_))

    val result=destData.mapPartitions(iter=>{
      //获取广播变量的值
      val broadcastValue: Array[(String, String, String, String, String)] = cityIpBroadcast.value
      //遍历迭代器获取每一个ip地址
      iter.map(ip=>{
        //将IP地址转化为Long
        val ipNum:Long=ipToLong(ip)
        //拿到long类型数字去广播变量中进行匹配，利用二分查询
        val index:Int=binarySearch(ipNum,broadcastValue)
        //返回结果数据  ((经度，维度)，1)
        ((broadcastValue(index)._4,broadcastValue(index)._5),1)
      })
    })
    //6、把相同经度和维度出现的次数累加
    val finalResult = result.reduceByKey(_+_)
    //7、打印输出结果
    finalResult.collect().foreach(x=>println(x))
    //保存结果数据到mysql表中
    finalResult.foreachPartition(datatomysql)


    //关闭sc
    sc.stop()
  }

}

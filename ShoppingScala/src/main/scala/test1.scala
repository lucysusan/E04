import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object test1 {
  def main(args: Array[String]): Unit ={
    /*
    if(args.length<2){
       System.err.println("Usage: <num> <output path>") //all fixed
       System.exit()
     }
     */

    val conf = new SparkConf().setAppName(Scala_test_1_1)
    val sc = new SparkContext(conf)
    //val df = spark.read.format("csv").option("header","true").load("hdfs:///e04/data/user_log_format1.csv")
    val data = sc.textFile("hdfs:///e04/data/user_log_format1.csv").flatMap(_.split("\n"))
    //delete first line
    val arr = data.take(1)
    val data1 = data.filter(!arr.contains(_)).filter(line=>line.split(",")(5).equals("1111")).map{
      line => (line.split(",")(1), line.split(",")(6))
    }.mapValues(_toInt)
    //omit values equal to 0
    val data2 = data1.filter(value => value._2>0)
    val data3 = data2.countByKey().toSeq.sortWith(_._2>_._2).take(100) //here fix num=100
    //seq -> rdd -> textfile
    val data4 = sc.parallelize(data3)
    data4.saveAsTextFile("hdfs:///e04/output1-1") //here fix output path
    sc.stop()
  }
}

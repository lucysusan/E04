import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object test1 {
 /*
  def main(args: Array[String]): Unit ={
    if(args.length<2){
      System.err.println("Usage: <num> <output path>") //all fixed
      System.exit()
    }
  */

    val conf = new SparkConf().setAppName(Scala_test_1_2)
    val sc = new SparkContext(conf)
  // read in csv as dataframe
    val dflog = spark.read.format("csv").option("header","true").load("hdfs:///e04/data/user_log_format1.csv")
    val dfinfo = spark.read.format("csv").option("header","true").load("hdfs:///e04/data/user_info_format1.csv")
  //first clean up the two tables separately
    val val dfia = dfinfo.filter("age_range<4 and age_range>0").select("user_id","age_range")
   `val dfla = dflog.filter("time_stamp=1111 and action_type!=0").select("user_id","seller_id","action_type")
  //join by user_id
    val dfjoin = dfia.join(dfla,"user_id")
  //count by seller_id
    val dfss = dfjoin.groupBy("seller_id").count()
  //orderby count in desc and take first 100
    val rddss = dfss.orderBy(dfss("count").desc).rdd.map(x=>(x(0),x(1))).take(100)
  //save as testFile
    sc.parallelize(rddss).saveAsTextFile("hdfs:///e04/output1-2")
  sc.stop()
  }
}

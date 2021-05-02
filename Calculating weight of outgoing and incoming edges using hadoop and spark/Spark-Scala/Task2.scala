import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))
    val file = sc.textFile("hdfs://localhost:9000" + args(0))
    val tgt = file.map(line=> line.split("\t") match { case Array(x,y,z) => (y.toInt,z.toInt) } )
    val source = file.map(line=> line.split("\t") match { case Array(x,y,z) => (x.toInt,-z.toInt) } )
    val output = tgt.++(source)
    output.cache()
    val edge = output.reduceByKey((a, b) => a + b).filter(_._2 % 2 != 0).map(x=>x._1+"\t"+x._2)
    edge.collect()
    edge.saveAsTextFile("hdfs://localhost:9000" + args(1))
  }
}

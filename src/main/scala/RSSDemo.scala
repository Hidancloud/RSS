import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object RSSDemo {
  def main(args: Array[String]) {
    println("Beginning")
    val durationSeconds = 20 //60
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
    println("Middle")

    // val urlCSV = args(0)
    val urls = Array("https://queryfeed.net/tw?token=5bfec0d2-4657-4d2a-98d0-69f3584dc3b3&q=%23Dota2") //urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds, connectTimeout=10000, readTimeout = 10000)
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      // rdd.take(15).foreach(println)
      for (el <- rdd.take(15)){
        println(el.description.value)
        println(parse(el.description.value))
      }
      println("____________after<______")
      //println(rdd.take(15).foreach(println))
      println("___________end___________")
      // import spark.sqlContext.implicits._
      // rdd.toDS().show()
    })
    println("AlmostEnd!")
    // run forever
    ssc.start()
    ssc.awaitTermination()
  }

  def parse(str: String):String = {
    var ans = ""
    val arr = str.split("<")
    for (el <- arr) {
      val i = el.split(">")
      if (i.size > 1) {
        ans += i(1)
      }
    }
    return ans
  }


}
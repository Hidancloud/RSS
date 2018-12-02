import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.functions._

import org.apache.spark.sql.functions.udf


object RSSDemo {
  def main(args: Array[String]) {
    println("Beginning")
    val durationSeconds = 60
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
    val model = new PredictionModel(PipelineModel.load("hdfs:///kazan/lr_model"), spark)
    sc.setLogLevel("ERROR")
    println("Middle")

    val theme = "School"
    val urls = Array("https://queryfeed.net/tw?token=5bfec0d2-4657-4d2a-98d0-69f3584dc3b3&q=%23" + theme) //urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds, connectTimeout=10000, readTimeout = 10000)
    stream.foreachRDD(rdd=>{


      // rdd.take(15).foreach(println)
      for (el <- rdd.take(15)){
        var parsed = parse(el.description.value)
        var (text, sentiment) = model.predict(parsed)
        println(sentiment, text)
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
    var fin_ans = ""
    val arr1 = ans.split("#")
    for (e1 <- arr1) {
      fin_ans += e1
    }
    return fin_ans
  }

  def createPredictionModel(spark: SparkSession): Unit = {


    var training_data = spark.read.format("csv")
      .option("header", "true")
      .load("hdfs:///Sentiment/twitter/train.csv")
      .toDF("id", "label", "text")

    val remove_dublicates:  (String) => String = (str) => {

      val sb = new StringBuilder()
      sb.append(' ')
      var lastchar = ' '
      var flag = false

      str.foreach(c => {
        if (!flag) {
          if (c == '!') {
            sb.append(" ! ")
            lastchar = '!'

          } else {
            if (c == '.') {
              sb.append(" . ")
              lastchar = '.'
            }else {
              if (c == '@'){
                flag = true
                lastchar = '@'
              }
            }
          }

          if (lastchar != c) {
            sb.append(c)
            lastchar = c
          }
        }else{
          if (c == ' '){
            flag = false
            lastchar = ' '
            sb.append(lastchar)
          }
        }



      })


      sb.toString()


    }

    val removeUDF = udf(remove_dublicates)


    training_data = training_data.withColumn("labelData", training_data("label").cast(IntegerType))
      .drop("label")
      .withColumnRenamed("labelData", "label")

    training_data = training_data.withColumn("text2", lower(col("text")))

    training_data = training_data.withColumn("new_text", removeUDF(training_data("text2")))

    training_data = training_data.drop("text2").drop("text")

    training_data = training_data.withColumnRenamed("new_text", "text")

    //    val Array(training, test) = training_data.randomSplit(Array(0.98, 0.02), seed = 1)


    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(3000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
      .setElasticNetParam(0.8)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(training_data)
    //    val model = pipeline.fit(training)

    model.write.overwrite().save("hdfs:///kazan/lr_model")


    //
    //    var trues = 0.0
    //    var all = 0.0
    //
    //    model.transform(test)
    //      .select("id", "label", "prediction")
    //      .collect()
    //      .foreach { case Row(id: String, label: Integer, prediction: Double) => {
    //        if (label == prediction.toInt) {
    //          trues = trues + 1
    //        }
    //        all = all + 1
    //
    //        println(s"$id, label=$label, prediction=$prediction")
    //      }
    //      }
    //
    //    println("---------------------------------------------------")
    //
    //    print(trues/all)
    //    println("---------------------------------------------------")




  }


}
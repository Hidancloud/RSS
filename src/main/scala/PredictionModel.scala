import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf



class PredictionModel(val model: PipelineModel, val spark: SparkSession) {

  val remove_dublicates:  (String) => String = (str) => { //more preprocessing

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

  def predict(text: String): (String, String) = {
    import spark.implicits._
    var df = Seq(text).toDF("text")
    df = df.withColumn("text2", lower(col("text")))
    df = df.withColumn("new_text", removeUDF(df("text2")))
    df = df.drop("text2").drop("text")
    df = df.withColumnRenamed("new_text", "text")
    var return_value = -1.0

    model.transform(df)
      .select("prediction")
      .collect()
      .foreach { case Row(prediction: Double) => {
          return_value = prediction
        }
      }
    return (text, return_value.toInt.toString)
  }


}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row

class PredictionModel(val model: PipelineModel, val spark: SparkSession) {

  def predict(text: String): (String, String) = {
    import spark.implicits._
    val df = Seq(text).toDF("text")

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
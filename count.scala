import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.{SQLContext, SparkSession}
object count extends {
  def main(args: Array[String]) {
    val sc = SparkSession.builder().appName("Sentiment")
      //.master("local")
      .getOrCreate();

    val parqfile = sc.read.json(args(0)).toDF()
    //using sql on dataframe
    parqfile.createOrReplaceTempView("table1")
    val reviews = sc.sql("SELECT text,stars FROM table1  WHERE text != '' LIMIT 50000")
    //val reviewsdf = reviews.withColumn("label", when(col("stars") >= 3.0, 1.0).otherwise(0.0))
    val reviewsdf = reviews.withColumn("label", when(col("stars") >= 3.5, 2.0).otherwise(when(col("stars")>=2.5,1).otherwise(0)))

    val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("words").setPattern("\\W")
    val countTokens = udf { (words: Seq[String]) => words.length }
    val regexTokenized = regexTokenizer.transform(reviewsdf)
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
    val nostopwords = remover.transform(regexTokenized)
    nostopwords.select("text", "filtered").withColumn("tokens", countTokens(col("filtered"))).show(false)
    val result = nostopwords.select("label", "filtered")

      val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("filtered").setOutputCol("rawfeatures")
      .setVocabSize(1000)
      .setMinDF(2)
      .fit(result)

    val featurizedData = cvModel.transform(result)
    featurizedData.show()
    val idf = new IDF().setInputCol("rawfeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()

    val splits = rescaledData.randomSplit(Array(0.9, 0.1))
    val (training, test) = (splits(0), splits(1))

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val pipeline = new Pipeline().setStages(Array(lr))

    val model = pipeline.fit(training)

    val predictions = model.transform(test)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error for LogisticRegression = " + (1.0 - accuracy))

    val nb = new NaiveBayes()
    val sec_model = nb.fit(training)

    val sec_predict = sec_model.transform(test)

    val evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val acc = evaluator2.evaluate(sec_predict)
    println("Test Error for NaiveBayes = " + (1.0 - acc))


  }
}
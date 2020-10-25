import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.math.{acos,sin,cos}
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.expressions.Pi
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.lower
import play.api.libs.json.Json


object Business {


  //expressions
  def main(args: Array[String]) {


    val spark = SparkSession.builder.appName("BusinessData").master("local").getOrCreate()

        // To filter by US States only
        val stateList = List("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MH", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "PW", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY")
       // val wordList =List("Restaurant","Cafe","Baker","Nightlife","Bars","Coffee","Tea","Dessert","Ice Cream","Juice")    // To filter only restaurant or food business

        var states = scala.collection.mutable.Map[String, String]()
        states++=List("Alabama"->"AL","Alaska"->"AK","Alberta"->"AB","American Samoa"->"AS","Arizona"->"AZ","Arkansas"->"AR","Armed Forces (AE)"->"AE","Armed Forces Americas"->"AA","Armed Forces Pacific"->"AP","British Columbia"->"BC","California"->"CA","Colorado"->"CO","Connecticut"->"CT","Delaware"->"DE","District Of Columbia"->"DC","Florida"->"FL","Georgia"->"GA","Guam"->"GU","Hawaii"->"HI","Idaho"->"ID","Illinois"->"IL","Indiana"->"IN","Iowa"->"IA","Kansas"->"KS","Kentucky"->"KY","Louisiana"->"LA","Maine"->"ME","Manitoba"->"MB","Maryland"->"MD","Massachusetts"->"MA","Michigan"->"MI","Minnesota"->"MN","Mississippi"->"MS","Missouri"->"MO","Montana"->"MT","Nebraska"->"NE","Nevada"->"NV","New Brunswick"->"NB","New Hampshire"->"NH","New Jersey"->"NJ","New Mexico"->"NM","New York"->"NY","Newfoundland"->"NF","North Carolina"->"NC","North Dakota"->"ND","Northwest Territories"->"NT","Nova Scotia"->"NS","Nunavut"->"NU","Ohio"->"OH","Oklahoma"->"OK","Ontario"->"ON","Oregon"->"OR","Pennsylvania"->"PA","Prince Edward Island"->"PE","Puerto Rico"->"PR","Quebec"->"QC","Rhode Island"->"RI","Saskatchewan"->"SK","South Carolina"->"SC","South Dakota"->"SD","Tennessee"->"TN","Texas"->"TX","Utah"->"UT","Vermont"->"VT","Virgin Islands"->"VI","Virginia"->"VA")
        val business_data= spark.read.json(args(0)).toDF()
        business_data.show(10)
        val business_df = business_data.select("business_id","name","stars","review_count","latitude","longitu de","city","state","categories","is_open")   // Read Business Data
        val census_df = spark.read.option("header", "true").csv(args(1)).toDF().select("State","County","TotalPop","Men","Women","Hispanic","White","Black","Native","Asian","Pacific") //Read Census Data

        val stateCode = udf {(state: String) => states.get(state)}            // To convert US State names in Census_Data to State Code
        val census_data = census_df.withColumn("state", stateCode(census_df("State")))        //creating new column state with state codes

        val categorycount = udf{(category:String) => category.toString.replace("Restaurants","").replace("Food","").split(", ").size}
       val us_state = business_df.filter(business_df("state").isin(stateList:_*) &&( business_data("categories").contains("Restaurant") || business_data("categories").contains("Cafe") || business_data("categories").contains("Bars")||business_data("categories").contains("Dessert")||business_data("categories").contains("Baker")||business_data("categories").contains("Fast Food")||business_data("categories").contains("Cafe")) && business_df("is_open")===1)
        val total_ids = us_state.count()
        val totalReviewCount = us_state.agg(sum("review_count")).first.get(0).toString            //Finding total number of review count across the document
        // Normalizing stars based on review count and stars given to the restaurant
        val normalizedBusinessData = us_state.withColumn("WeightedRate",((us_state("review_count")/(us_state("review_count")+10))* us_state("stars") + (lit(10).divide(us_state("review_count")+10))*(totalReviewCount.toInt/total_ids)))
        val categorycount1 = udf{(category:String) => category.toString.split(", ")}

        val temp2=normalizedBusinessData.withColumn("categories", explode(categorycount1(col("categories"))))
        val temp3 =temp2.groupBy("city","state").pivot("categories").avg("stars")

//     temp2.write.format("com.databricks.spark.csv").save("categories.csv")

      val final_business = normalizedBusinessData.withColumn("categories_count",categorycount(normalizedBusinessData("categories")))






     //        val lat_lon_businessId = normalizedBusinessData.select("latitude","longitude","business_id").rdd.map(x=> (x.get(0),x.get(1),x.get(2))).collect()      //Collecting latitude longitude
     //        var all_county = List.empty[(String,String)]
     //        lat_lon_businessId.foreach{ case(a,b,c)=>
     //          var url = "https://geo.fcc.gov/api/census/block/find?latitude="+a+"&longitude="+b+"&showall=true&format=json"
     //          var result= Json.parse(scala.io.Source.fromURL(url).mkString)
     //          var county_name =((result\ "County" \ "name").as[String].concat(" County"),c.toString)
     //          all_county=county_name::all_county
     //          }
     import spark.implicits._
     //        val county = all_county.toDF("County","business_id")
     //        val county_data= county.join(normalizedBusinessData,"business_id")
     //        val business_census= normalizedBusinessData.join(census_data,Seq("County","state"))
           final_business.write.format("com.databricks.spark.csv").save("business1.csv")
//             business_census.write.format("com.databricks.spark.csv").option("header",true).save("business_census.csv")
   }
}
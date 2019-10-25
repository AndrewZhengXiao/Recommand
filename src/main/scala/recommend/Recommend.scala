import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, functions}

import scala.collection.mutable.ListBuffer
import scalikejdbc._
import scalikejdbc.config._



object Recommend {

  def HotNews(spark:SparkSession, HotNewsPath:String, topN:Int = 50)={
    /**
     * 输入热点新闻来源，以及取当前TopN的值
     * 按照时间戳倒叙，选取最近的TopN的热点新闻
     * 输出排序后的 top N个热点新闻
     */
    val HotNews =  spark.read.format("com.databricks.spark.csv")
      .option("inferSchema",  false)
      .option("header", true)
      .option("nullValue", "\\N")
      .option("escape", "\"")
      .option("quoteAll", "true")
      .option("sep", ",")
      .csv(HotNewsPath)

    HotNews.createOrReplaceTempView("HotNews")
//    HotNews.orderBy(HotNews("date").desc).show(50)
    var res = HotNews.orderBy(HotNews("date").desc).limit(topN).select("title","url","date")

    res
  }

  def UserCF_Train(spark: SparkSession, UserProfile_Path:String,Model_Path:String,ID_Path:String,Tag_Path:String): Unit ={
    /**
     * 输入用户画像结果，用户ID，最终TopN相似文章的数量，topK相似用户的数量
     * 用户相似度，取得最相似N个用户的行为文章tag
     * 通过tag取得最终topK个文章
     */
    import spark.implicits._

    val UserProfile =  spark.read.format("com.databricks.spark.csv").csv(UserProfile_Path)
    var i = 0

    var userProfile = UserProfile.na.drop(Array("_c17"))  //去除空值id
    userProfile.select("_c0", "_c17").createOrReplaceTempView("data")

    //解析Datafram 读取用户Id,文章tag,和分数
    val UserData =
      spark.sql("select * from data").rdd.flatMap(row => {
      val id: String = row.getString(0)
      val str = row.getString(1)
      val tuples: ListBuffer[(String, String, Double)] = new ListBuffer[(String,String,Double)]
      val strings: Array[String] = str.replaceAll("SortClassJson", "").split("\\), \\(")
//      println(str.replaceAll("SortClassJson", ""))
      for(s <- strings){
        if(s.length > 5) {
//          println(s,s.indexOf("(") + 1, s.indexOf(","))
          val k = s.substring(s.indexOf("(") + 1, s.indexOf("{")-1)
          val json = JSON.parseObject(s.replaceAll("\\)]","").substring(s.indexOf("{"))).getDouble("score")
          tuples.append((id, k, json))
        }
      }
      tuples
    }).toDF("id","tag","score")
    UserData.createOrReplaceTempView("training")

    //遍历用户ID和TAG 生成对应映射表
    val SaveData_id = UserData.select("id").distinct().rdd.map(row => {
      val id = row.getString(0)
      i+=1
      (i,id)
    }).toDF("int_id", "uu_id")
    SaveData_id.createOrReplaceTempView("UserID")
    SaveData_id.write.mode(SaveMode.Overwrite).json(ID_Path)
    i = 0
    val SaveData_tag = UserData.select("tag").distinct().rdd.map(row => {
      val tag = row.getString(0)
      i+=1
      (i,tag)
    }).toDF("int_id", "uu_id")
    SaveData_tag.createOrReplaceTempView("TagID")
    SaveData_tag.write.mode(SaveMode.Overwrite).json(Tag_Path)


    //替换原表，生成训练集
    var UserTrainning = spark.sql("select UserID.int_id as id,TagID.int_id as tag,score from training join UserID on training.id = UserID.uu_id join TagID on training.tag = TagID.uu_id")
//    UserTrainning.show()

    val Array(training, test) = UserTrainning.randomSplit(Array(0.8, 0.2))
    val alsExplicit = new ALS().setMaxIter(20).setRegParam(0.01).setUserCol("id"). setItemCol("tag").setRatingCol("score")
    val model: ALSModel = alsExplicit.fit(training)
    model.write.overwrite().save(Model_Path)
    model.setColdStartStrategy("drop")
    val frame: DataFrame = model.recommendForAllUsers(50)
    frame.write.mode(SaveMode.Overwrite).json("hdfs://10.10.10.233:9000/YouLiaoData/Rec_res/")

//    val predictionsExplicit = modelExplicit.transform(test)
//    predictionsExplicit.show()
  }

  def UserCF_Predict(spark: SparkSession,Model_Path:String,ID_Path:String,Tag_Path:String,TopN:Int = 50, UserID:String):DataFrame={
    val als = ALSModel.load(Model_Path)
    als.setColdStartStrategy("drop")
    var ID= spark.read.json(ID_Path)
    var Tag= spark.read.json(Tag_Path)
//    ID.show()
    Tag.createOrReplaceTempView("Tag_ori")
    import spark.implicits._
    var User = ID.where(s"uu_id = '$UserID'").select("int_id" ).toDF("id")
    if(User.count()>0){
      val Id = User.limit(1)
      val userSubsetRecs = als.recommendForUserSubset(Id,TopN)
//      userSubsetRecs.show(false)
      userSubsetRecs.withColumn("tup", functions.explode(functions.col("recommendations")))
        .rdd.map(row => {
        val row1 = row.getStruct(2)
        (row1.getInt(0),row1.getFloat(1))
      }).toDF("tag_id", "score").createOrReplaceTempView("tagTable")
      val TagRes: DataFrame = spark.sql("select Tag_ori.uu_id as tag from Tag_ori join tagTable on tagTable.tag_id = Tag_ori.int_id")
//      TagRes.show(false)
      TagRes
    }
    else {
      null
    }



  }

  def MysqlTest(spark: SparkSession,ID_Path:String,Tag_Path:String):DataFrame={
    case class Employer(name: String, age: Int, salary: Long)
    var ID= spark.read.json(ID_Path)
    var Tag= spark.read.json(Tag_Path)
    ID.show()
    Tag.show()
    Tag.createOrReplaceTempView("Tag_ori")


    DBs.setup()
    val config = DBs.config


    null

  }


  def main(args: Array[String]): Unit = {
    //配置spark
//    var Spark_Path = "spark://10.10.10.233:7077"      //args(0)
    var Spark_Path = "local"      //args(0)
    var Persona_Path =  "hdfs://10.10.10.233:9000/YouLiaoData/Persona/" //args(1)
    var Action_Path = "hdfs://10.10.10.233:9000/YouLiaoData/ActionUser/" //args(2)
    var UserProfile_Path = "hdfs://10.10.10.233:9000/YouLiaoData/Result/" //args(3)
    var HotNews_path = "hdfs://10.10.10.233:9000/YouLiaoData/HotNews" //args(4)
    var Model_Path = "hdfs://10.10.10.233:9000/YouLiaoData/Model/CF_model" //args(5)
    val ID_Path = "hdfs://10.10.10.233:9000/YouLiaoData/ID/" //args(6)
    val Tag_Path = "hdfs://10.10.10.233:9000/YouLiaoData/TAG/"//args(7)
    var TopN_HotNews = 50 //args(8)
    var TopN_User = 50 //args(9)
    var TopK_UserTitle = 10 //args(10)


//
//    var Spark_Path = args(0)
//    var Persona_Path =  args(1)
//    var Action_Path = args(2)
//    var UserProfile_Path = args(3)
//    var HotNews_path = args(4)
//    var Model_Path = args(5)
//    val ID_Path = args(6)
//    val Tag_Path = args(7)
//    var TopN_HotNews = args(8).toInt
//    var TopN_User = args(9).toInt
//    var TopK_UserTitle = args(10).toInt



    val spark = SparkSession.builder().appName("Recommender").master(Spark_Path).getOrCreate()
    import spark.implicits._
    Predef.println("spark version: " + spark.version)

//    var HotNews_res = HotNews(spark,HotNews_path,TopN_HotNews)
//    HotNews_res.show(TopN_HotNews)

//    UserCF_Train(spark,UserProfile_Path,Model_Path,ID_Path,Tag_Path)
//    var CF_res = UserCF_Predict(spark,Model_Path,ID_Path,Tag_Path,TopN_User,UserID="5d40dd11ac0244003501dfd0")
//    CF_res.show()

    var test = MysqlTest(spark,ID_Path,Tag_Path)
    //    User_res.show(TopK_UserTitle)


    spark.close()

  }

}

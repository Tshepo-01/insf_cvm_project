package cvm_insf

import org.apache.spark.sql.{SaveMode, SparkSession}
import insfTransformation.{spark, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import Arguments._
object dataExtraction {

  val spark: SparkSession = SparkSession.builder().appName("Installment Finance Application of CustomerXperience CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {


    if(!isArgsValid(args)){

      throw  new Exception("The values passed are not valid")
    }

    val insfPublishPath = args(0)
    val accountStatusPath = args(1)
    val sourceAccountTypePath = args(2)
    val sourceAccountStatusPath = args(3)
    val productCodePath = args(4)
    val InsfStagingPath = args(5)
    val statgingException = args(6)
    val InsfReportingPath = args(7)
   // val tableName= args(8)
    //Dm9s
    val dm9Path = args(8)


    val insfTempTBL = spark.read.parquet(insfPublishPath)
    insfTempTBL.printSchema()
    val  accountStatusTBL = spark.read.option("header", true).csv(accountStatusPath)
    val sourceAccounTypeTBL = spark.read.option("header",true).csv(sourceAccountTypePath)
    val sourceAccountStatusTBL = spark.read.option("header",true).csv(sourceAccountStatusPath)
    val productCodeTBL = spark.read.option("header",true).csv(productCodePath)

    //DM9
    val dm9TBL = spark.read.parquet(dm9Path)

    val insfRawTBL = pullInsf(insfTempTBL)

    //Filtering to ExceptionFile
    val exFile = filteringException(insfRawTBL)
    exFile.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).csv(statgingException)
    exFile.createOrReplaceTempView("filteringCOunt")
    val r_count= spark.sql("select count(*) as excepTionsCount from filteringCOunt ")
    r_count.toJavaRDD.saveAsTextFile(statgingException+"/"+"_exceptionFileCount")
    insfRawTBL.printSchema()
    import spark.implicits._
    insfRawTBL.createOrReplaceTempView("tempTable")
    val recount = spark.sql("select count(*) as stagingCount from tempTable")
     recount.toJavaRDD.saveAsTextFile(statgingException+"/"+"_sourceCount")

    //Loading Raw data to Staging
    insfRawTBL.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).csv(InsfStagingPath)

    //insfTBL.filter(col("SOURCE_ACCOUNT_STATUS_CODE").isin(0,2, 3, 4, 5, 6, 7, 8))
    //insfTBL.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).csv(statgingException)
    //count before processing
    //insfTBL.createOrReplaceTempView("beforeTBL")
    //val beforeCount = spark.sql("select count(*) from beforeTBL")
    //beforeCount.toJavaRDD.saveAsTextFile(statgingException+"/"+"_BeforeProcess")

    //Intergration of filtered Insf to DM9sa and applying of LoookUp tables
    val cleanInsf = filteringStaging(insfRawTBL)
    val combinedtoDM9 = combinewithDM9(cleanInsf,dm9TBL)
    val finalResult = joinIndfTBL(combinedtoDM9, accountStatusTBL, sourceAccounTypeTBL,sourceAccountStatusTBL, productCodeTBL)
    finalResult.printSchema()
      /*finalResult.filter(! col("SOURCE_ACCOUNT_STATUS_CODE").isin(0,2, 3, 4, 5, 6, 7, 8))
    finalResult.filter($"OPEN_DATE"=!="9999-01-01").repartition(2).write.option("header",true).mode(SaveMode.Overwrite).csv(InsfStagingPath)
    val x = finalResult.filter(!finalResult("SOURCE_ACCOUNT_STATUS_CODE").isin(0,2, 3, 4, 5, 6, 7, 8) and finalResult("OPEN_DATE")==="9999-01-0l")
    x.toDF().createOrReplaceTempView("ifTable")
    val rCount = spark.sql("Select count(*), sum(ACCOUNT_BALANCE) from ifTable")
    rCount.toJavaRDD.saveAsTextFile(statgingException+"/"+"_controlFile")
    x.toDF().repartition(2).write.option("header",true).mode(SaveMode.Overwrite).parquet(InsfReportingPath)*/
    finalResult.repartition(2).write.mode(SaveMode.Overwrite).parquet(InsfReportingPath)
    finalResult.createOrReplaceTempView("ifTBL")
    val rCount = spark.sql("select count(*) as parquetCount from ifTBL ")
    rCount.toJavaRDD.saveAsTextFile(statgingException+"/"+"_ParquetCount")
    spark.stop()
    //Loading Table
    //spark.sql("MSCK REPAIR TABLE "+ tableName)
    //spark.sql("MSCK REPAIR TABLE "  + tableNameNotice)
    //spark.sql("REFRESH TABLE "+ tableName)
    //spark.sql("REFRESH TABLE "+ tableNameNotice)
  }

}

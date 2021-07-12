package cvm_insf

import org.apache.spark.sql.{SaveMode, SparkSession}
import insfTransformation._
import Arguments._
object dataExtraction {

  val spark: SparkSession = SparkSession.builder().appName("Installment Finance Application of CustomerXperience CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
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
    val InsfReportingPath = args(6)

    val insfTempTBL = spark.read.parquet(insfPublishPath)
    val  accountStatusTBL = spark.read.option("header", true).csv(accountStatusPath)
    val sourceAccounTypeTBL = spark.read.option("header",true).csv(sourceAccountTypePath)
    val sourceAccountStatusTBL = spark.read.option("header",true).csv(sourceAccountStatusPath)
    val productCodeTBL = spark.read.option("header",true).csv(productCodePath)

    val insfTBL = pullInsf(insfTempTBL)
    insfTBL.printSchema()
    insfTBL.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).csv(InsfStagingPath)

   /* val insfFinalTBL = spark.read.option("header",true).csv(InsfStagingPath)
    insfFinalTBL.printSchema()*/

    val finalResult = joinIndfTBL(insfTBL, accountStatusTBL, sourceAccounTypeTBL, sourceAccountStatusTBL, productCodeTBL)
    finalResult.printSchema()
    finalResult.repartition(2).write.option("header",true).mode(SaveMode.Overwrite).parquet(InsfReportingPath)
  }

}

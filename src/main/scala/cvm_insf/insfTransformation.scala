package cvm_insf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.tools.nsc.io.Path

object insfTransformation {

  val spark: SparkSession = SparkSession.builder()
    .appName("Installment Finance Application of CustomerXperience CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  def pullInsf(df: DataFrame): DataFrame = {

    import spark.implicits._

   val insf = df.select($"FINTD121".cast(IntegerType).as("FINANCE_TYPE"),

     ($"capod121" +$"scpod121" + $"baldd121"+$"intod121"+$"sinod121").as("ACCOUNT_BALANCE"),

      lpad($"acnod121".cast(StringType),25, "0").as("ACCOUNT_NUMBER"),
      $"acstd121".cast(StringType).as("SOURCE_ACCOUNT_STATUS_CODE"),
      $"actpd121".cast(StringType).as("SOURCE_ACCOUNT_TYPE_CODE"),
      when($"acstd121".isin( 97,98,99,10),
     to_date(date_format($"dtscd121","yyyy-MM-dd"))).as("CLOSED_DATE"),

      concat(rpad(ltrim($"CLCAD121"), 7 , " ") , lpad(ltrim($"clcnd121"), 3, "0")).as("CIF_CUSTOMER_KEY"),
      to_date(date_format($"enceladus_info_date","yyyy-MM-dd")).as("INFORMATION_DATE"),
      to_date(date_format($"efdcd121","yyyy-MM-dd")).as("OPEN_DATE"),
      $"siald121".cast(StringType).as("SITE_CODE"),

      $"CAPOD121".cast(DecimalType(38,10)).as("CAPITAL_OUSTANDING"),
      $"scpod121".cast(DecimalType(38,10)).as("SUPPLEMENTRY_CAPITAL_OUTSTANDING"),
      $"baldd121".cast(DecimalType(38,10)).as("BALANCE_DUE"),
      $"intod121".cast(DecimalType(38,10)).as("INTEREST_OUSTANDING"),
      $"sinod121".cast(DecimalType(38,10)).as("SUPPLEMENTARY_INTEREST_OUTSTANDING"),
      to_date(date_format($"dtlid121","yyyy-MM-dd")).as("EXPIRY_DATE")
     ).withColumn("PRE_PAYMENT_AMOUNT",lit("").cast(DecimalType(38,10)))
      .withColumn("PRODUCT_CODE",lit("LOAN").cast(StringType))

      .withColumn("PRODUCT_SUB_STATUS",lit(null).cast(StringType))
      val x = insf.na.fill(0,Array("FINANCE_TYPE")).na.fill(0,Array("SOURCE_ACCOUNT_TYPE_CODE"))
    x
    //val handleNull = filteringIF.na.fill(0,Array("FINANCE_TYPE"))
    //.filter(! col("SOURCE_ACCOUNT_STATUS_CODE").isin(0,2, 3, 4, 5, 6, 7, 8))
    //.filter($"OPEN_DATE"=!="9999-01-01")
    //handleNull
  }
  def filteringException(df:DataFrame):DataFrame ={

    df.createOrReplaceTempView("exceptionFile")
    val filteringQuery = spark.sql("select * from exceptionFile " +
      "where SOURCE_ACCOUNT_STATUS_CODE in (0,2, 3, 4, 5, 6, 7, 8) " +
      " and OPEN_DATE='9999-01-01'")
    filteringQuery

  }
  //Demo for Github
  def showcaseDemo(df:DataFrame):DataFrame ={
    df
  }

  def filteringStaging(df:DataFrame):DataFrame={

    df.createOrReplaceTempView("exceptionFile")
    val filteringQuery = spark.sql("select * from exceptionFile " +
      "where SOURCE_ACCOUNT_STATUS_CODE not in (0,2, 3, 4, 5, 6, 7, 8) " +
      " and OPEN_DATE<>'9999-01-01'")
    filteringQuery
  }

  def combinewithDM9(df:DataFrame, dm9:DataFrame):DataFrame ={
    import spark.implicits._
    val combinedTBL = df.join(dm9, df("ACCOUNT_NUMBER")===dm9("DM9_ACCOUNT_NUMBER"),"Left").drop(dm9("DM9_ACCOUNT_NUMBER"))
    combinedTBL
  }
  def joinIndfTBL(df : DataFrame,accountStatusLookup : DataFrame, sourceAccountTypeLookup: DataFrame, sourceAccountStatusLookup: DataFrame , productCodeLookup: DataFrame): DataFrame = {
    import spark.implicits._
    val joinDF = df.join(accountStatusLookup, df("SOURCE_ACCOUNT_STATUS_CODE").as("CODE")
      === accountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE").as("IF_CODE"),"Left")
      .drop(accountStatusLookup("Source"))
      .drop(accountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE"))

    val joinDF1 = joinDF.join(sourceAccountTypeLookup, joinDF("SOURCE_ACCOUNT_TYPE_CODE").as("C1").cast(StringType)
      === sourceAccountTypeLookup("SOURCE_ACCOUNT_TYPE_CODE").as("C2").cast(StringType)
      && joinDF("FINANCE_TYPE").cast(IntegerType).as("F1") === sourceAccountTypeLookup("FINANCE_TYPE").cast(IntegerType).as("F2"), "Left")
      .drop(sourceAccountTypeLookup("SOURCE_ACCOUNT_TYPE_CODE"))
      .drop(sourceAccountTypeLookup("FINANCE_TYPE"))
    //new Changes
    val joinDF2 = joinDF1.join(sourceAccountStatusLookup, joinDF1("SOURCE_ACCOUNT_STATUS_CODE").as("A1")
      === sourceAccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE").as("A2"), "Left")
      .drop(sourceAccountStatusLookup("SOURCE_ACCOUNT_STATUS_CODE"))

   val finalJoin = joinDF2.join(productCodeLookup,
     joinDF2("SOURCE_ACCOUNT_TYPE_CODE").as("S1").cast(StringType)
       === productCodeLookup("SOURCE_ACCOUNT_TYPE_CODE").as("S2").cast(StringType) &&
       joinDF2("FINANCE_TYPE").cast(IntegerType)===productCodeLookup("FINTD121").cast(IntegerType)
     && joinDF2("PRODUCT_CODE").as("P") === productCodeLookup("PRODUCT_CODE").as("C"), "Left")

     .drop(productCodeLookup("SOURCE_ACCOUNT_TYPE_CODE"))

     .drop(productCodeLookup("PRODUCT_CODE"))
     .drop("FINTD121","Source")

    finalJoin.withColumn("SOURCE_CODE",lit("INSF"))

  }
  //latestChanges to IF


}
package cvm_insf

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types._

object insfTransformation {

  val spark: SparkSession = SparkSession.builder().appName("Installment Finance Application of CustomerXperience CVM")
    .config("spark.debug.maxToStringFields", 200)
    .config("spark.sql.parquet.writeLegacyFormat",true)
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  def pullInsf(df: DataFrame): DataFrame = {
    import spark.implicits._

    df.select($"FINTD121".cast(StringType).as("FINANCE_TYPE"),
      $"FLIND121".cast(StringType).as("FLEXI_LOAN_FLAG"),
      ($"capod121" +$"scpod121" + $"baldd121").cast(DecimalType(15,2)).as("ACCOUNT_BALANCE"),
      lpad($"acnod121".cast(StringType),16, "0").as("ACCOUNT_NUMBER"),
      $"acstd121".cast(StringType).as("SOURCE_ACCOUNT_STATUS_CODE"),
      $"actpd121".cast(StringType).as("SOURCE_ACCOUNT_TYPE_CODE"),
      when($"acstd121" === 2 ,
     to_date(date_format($"dtscd121","yyyy-MM-dd"))).as("CLOSED_DATE"),
      concat(rpad(ltrim($"CLCAD121"), 7 , " ") , lpad(ltrim($"clcnd121"), 3, "0")).as("CUSTOMER_KEY"),
      to_date(date_format($"enceladus_info_date","yyyy-MM-dd")).as("INFORMATION_DATE"),
      to_date(date_format($"efdcd121","yyyy-MM-dd")).as("OPEN_DATE"),
      $"siald121".cast(StringType).as("SITE_CODE"),
      $"CAPOD121".cast(DecimalType(15,2)).as("INITIAL_INTEREST"),
      $"scpod121".cast(DecimalType(15,2)).as("SUPPL_CAPITAL_OUTSTANDING"),
      $"baldd121".cast(DecimalType(15,2)).as("BALANCE_DUE"),
      to_date(date_format($"dtlid121","yyyy-MM-dd")).as("CONTRACT_TERM_CLOSE_DATE")
     )


  }

  def joinIndfTBL(df : DataFrame,accountStatusLookup : DataFrame, sourceAccountTypeLookup: DataFrame, sourceAccountStatusLookup: DataFrame , productCodeLookup: DataFrame): DataFrame = {
    import spark.implicits._
    val joinDF = df.join(accountStatusLookup, $"SOURCE_ACCOUNT_STATUS_CODE" === accountStatusLookup("IF_ACCOUNT_STATUS_CODE"),"Left").drop("SOURCE")
    val joinDF1 = joinDF.join(sourceAccountTypeLookup, $"SOURCE_ACCOUNT_TYPE_CODE" === sourceAccountTypeLookup("IF_ACCOUNT_TYPE_CODE_SOURCE"), "Left")
    val joinDF2 = joinDF1.join(sourceAccountStatusLookup, joinDF1("ACCOUNT_STATUS_CODE")=== sourceAccountStatusLookup("IF_ACCOUNT_STATUS_CODE"), "Left")

   val finalJoin = joinDF2.join(productCodeLookup,
     joinDF2("SOURCE_ACCOUNT_TYPE_CODE").cast(StringType) === productCodeLookup("ACTPD121").cast(StringType) &&
     joinDF2("FINANCE_TYPE").cast(StringType)===productCodeLookup("FINTD121").cast(StringType)
     &&
     joinDF2("FLEXI_LOAN_FLAG").cast(StringType) ===productCodeLookup("FLIND121").cast(StringType), "Left")
     .drop("FINTD121","FLIND121","Source")

    finalJoin.drop("ACTPD121","IF_ACCOUNT_STATUS_CODE","IF_ACCOUNT_TYPE_CODE_SOURCE").withColumn("SOURCE_CODE",lit("INSF"))
  }

}

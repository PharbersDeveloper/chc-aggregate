package com.pharbers

import com.pharbers.aggregate.ppt.aggregateData
import com.pharbers.aggregate.util.saveDF2mongo

object chcToDisTest extends App {
//
//	import com.pharbers.data.util._
//	import com.pharbers.data.conversion._
//	import org.apache.spark.sql.functions._
//	import com.pharbers.data.util.ParquetLocation._
//	import com.pharbers.data.util.sparkDriver.ss.implicits._
//
//	sparkDriver.sc.addJar("/Users/cui/github/chc-aggregate/target/pharbers-chc-aggregate-1.0-SNAPSHOT.jar")
//	val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
//	val chcDF = CSV2DF(chcFile)
//	val chcCvs = CHCConversion()
//	val pdc = ProductDevConversion()(ProductImsConversion(), ProductEtcConversion())
//	val productDIS = pdc.toDIS(Map(
//		"productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
//		"productImsERD" -> Parquet2DF(PROD_IMS_LOCATION),
//		"oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION),
//		"atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
//	))("productDIS")
//	val chcDIS = chcCvs.toDIS(Map(
//		"chcERD" -> Parquet2DF(CHC_LOCATION),
//		"dateERD" -> Parquet2DF(CHC_DATE_LOCATION),
//		"cityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
//		"productDIS" -> productDIS
//	))("chcDIS")
//	val resultDF = chcDIS.select("PRODUCT_ID", "SALES", "UNITS", "PRODUCT_NAME", "MOLE_NAME", "PACKAGE_DES", "PACKAGE_NUMBER",
//		"CORP_NAME", "DELIVERY_WAY", "DOSAGE_NAME", "PACK_ID", "TIME", "name", "ATC3", "OAD_TYPE")
//		.withColumnRenamed("name", "CITY")
//		.withColumn("MARKET", lit("降糖药市场"))
//	val aggreagteDF = aggregateData().getAggregate(resultDF)
//	saveDF2mongo().saveDF(aggreagteDF, "aggregateData")
}

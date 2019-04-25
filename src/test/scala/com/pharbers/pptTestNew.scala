package com.pharbers

import com.pharbers.ahcaggregate.moudle.pptInputData
import com.pharbers.ahcaggregate.ppt.{aggregateData, generatePPTData}
import com.pharbers.ahcaggregate.util.saveDF2mongo

object pptTestNew extends App {

	import com.pharbers.data.util._
	import com.pharbers.data.conversion._
	import org.apache.spark.sql.functions._
	import com.pharbers.data.util.ParquetLocation._
	import com.pharbers.data.util.sparkDriver.ss.implicits._

	sparkDriver.sc.addJar("/Users/cui/github/chc-aggregate/target/pharbers-chc-aggregate-1.0-SNAPSHOT.jar")
	val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
	val chcDF = CSV2DF(chcFile)
	val chcCvs = CHCConversion()
	val pdc = ProductDevConversion()(ProductImsConversion(), ProductEtcConversion())
	val productDIS = pdc.toDIS(Map(
		"productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
		"productImsERD" -> Parquet2DF(PROD_IMS_LOCATION),
		"oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION),
		"atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
	))("productDIS")
	val chcDIS = chcCvs.toDIS(Map(
		"chcERD" -> Parquet2DF(CHC_LOCATION),
		"dateERD" -> Parquet2DF(CHC_DATE_LOCATION),
		"cityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
		"productDIS" -> productDIS
	))("chcDIS")
	val resultDF = chcDIS.select("PRODUCT_ID", "SALES", "UNITS", "PRODUCT_NAME", "MOLE_NAME", "PACKAGE_DES", "PACKAGE_NUMBER",
		"CORP_NAME", "DELIVERY_WAY", "DOSAGE_NAME", "PACK_ID", "TIME", "name", "ATC3", "OAD_TYPE")
		.withColumnRenamed("name", "CITY")
		.withColumn("MARKET", lit("降糖药市场"))
	val aggreagteDF = aggregateData().getAggregate(resultDF)
//	saveDF2mongo().saveDF(aggreagteDF, "aggregateData")
	val dataList = List("2017Q3YTD", "2018Q3YTD")
	val valueTypeList = List("sales", "share")
	val filterList: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("oad")),
		("key", List("其他", "GLP-1激动剂", "DPP4抑制剂", "格列酮类", "格列奈类", "磺脲类", "双胍类", "糖苷类")),
		("city", List("北京市")),
		("date", dataList),
		("valueType", valueTypeList)
	)
	val mergeList = List("date", "valueType")
	val poivtList = List("key")
	val sortMap = Map("asc" -> col("2018Q3YTDsales"))
	val selectList = List("key")
	val titleList = List(List("", "2017Q3YTD", "2018Q3YTD", "", ""))
	val limitNum_p2_t1 = 7
	val p2_t1 = pptInputData("p2_t1", dataList, valueTypeList, filterList, mergeList, poivtList, limitNum_p2_t1, sortMap,
		selectList, titleList)

	val sortMap_p2_t1 = Map("desc" -> col("2018Q3YTDsales"))
	val filterList_p3_t1 = List(("keyType", List("corp")),
		("city", List("北京市")),
		("date", List("2018Q3YTD")),
		("valueType", List("sales", "share", "growth"))
	)
	val limitNum_p3_t1 = 15
	val p3_t1 = pptInputData("p3_t1", List("2018Q3YTD"), List("share", "growth", "sales"), filterList_p3_t1, mergeList,
		poivtList, limitNum_p3_t1, sortMap_p2_t1, selectList, List(List("", "份额", "增长", "Size"))
	)

	val dataList_p3 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3")
	val filterList_p3 = List(
		("keyType", List("mole")),
		("city", List("北京市")),
		("date", dataList_p3),
		("valueType", valueTypeList)
	)

	val titleList_p2 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "", "", "", "", "", "", ""))
	val sortMap_p3_t1 = Map("asc" -> col("2018Q3sales"))
	val limitNum_p4_t1 = 9
	val p4_t1 = pptInputData("p4_t1", dataList_p3, valueTypeList, filterList_p3, mergeList, poivtList, limitNum_p4_t1,
		sortMap_p3_t1, selectList, titleList_p2)

	val filterList_p4_t1: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList),
		("valueType", valueTypeList)
	)
	val titleList_p4_t1 = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD2", "2018Q3YTD3"))
	val sortMap_p4_t1 = Map("asc" -> col("2018Q3YTDsales"))
	val limitNum_p5_t1 = 10
	val p5_t1 = pptInputData("p5_t1", dataList, valueTypeList, filterList_p4_t1, mergeList, poivtList, limitNum_p5_t1,
		sortMap_p4_t1, selectList, titleList)

	val dataList_p4_t2 = List("2018Q3YTD")
	val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
	val filterList_p4_t2: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList_p4_t2),
		("valueType", valueTypeList_p4_t2)
	)
	val limitNum_p5_t2 = 10
	val selectList_p4_t2 = List("key", "mole_name", "corp_name")
	val titleList_p4_t2 = List(List("重点产品", "分子", "公司","sales", "growth", "share", "shareGrowth", "EI"))
	val poivtList_p4_t2 = List("key", "mole_name", "corp_name")
	val sortMap_p4_t2 = Map("desc" -> col("2018Q3YTDsales"))
	val p5_t2 = pptInputData("p5_t2", dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2, mergeList, poivtList_p4_t2,
		limitNum_p5_t2, sortMap_p4_t2, selectList_p4_t2, titleList_p4_t2)

	val filterList_p5_1: List[(String, List[String])] = List(
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList),
		("valueType", valueTypeList),
		("mole_name", List("二甲双胍"))
	)
	val titleList_p5_t1 = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD2", "2018Q3YTD3"))
	val sortMap_p5_t1 = Map("asc" -> col("2018Q3YTDsales"))
	val limitNum_p6_t1 = 9
	val p6_t1 = pptInputData("p6_t1", dataList, valueTypeList, filterList, mergeList, poivtList, limitNum_p6_t1,
		sortMap_p5_t1, selectList, titleList_p5_t1)

	val dataList_p5_t2 = List("2018Q3YTD")
	val filterList_p5_t2: List[(String, List[String])] = List(
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList_p5_t2),
		("valueType", valueTypeList_p4_t2),
		("mole_name", List("二甲双胍"))
	)
	val selectList_p5_t2 = List("key", "mole_name", "corp_name")
	val sortMap_p6_t2 = Map("desc" -> col("2018Q3YTDsales"))
	val titleList_p6_t2 = List(List("重点产品", "分子", "公司","sales", "growth", "share", "shareGrowth", "EI"))
	val limitNum_p6_t2 = 9
	val p6_t2 = pptInputData("p6_t2", dataList_p5_t2, valueTypeList_p4_t2, filterList_p5_t2, mergeList, selectList_p5_t2,
		limitNum_p6_t2, sortMap_p6_t2, selectList_p5_t2, titleList_p6_t2)

	val dataList_p6_t1 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3")
	val valueTypeList_p6_t1 = List("sales")
	val filterList_p6_t1: List[(String, List[String])] = List(
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList_p6_t1),
		("valueType", valueTypeList_p6_t1),
		("mole_name", List("二甲双胍"))
	)
	val titleList_p6_t1 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3"))
	val sortMap_p7_t1 = Map("asc" -> col("2018Q3sales"))
	val limitNum_p7_t1 = 9
	val p7_t1 = pptInputData("p7_t1", dataList_p6_t1, valueTypeList_p6_t1, filterList_p6_t1, mergeList, poivtList,
		limitNum_p7_t1, sortMap_p7_t1, selectList, titleList_p6_t1)

	val valueTypeList_p7_t1 = List("sales", "share")
	val filterList_p7_t1: List[(String, List[String])] = List(
		("keyType", List("prod")),
		("city", List("北京市")),
		("date", dataList_p6_t1),
		("valueType", valueTypeList_p7_t1),
		("mole_name", List("二甲双胍"))
	)
	val sortMap_p8_t1 = Map("asc" -> col("2018Q3sales"))
	val limitNump8_t1 = 9
	val p8_t1 = pptInputData("p8_t1", dataList_p6_t1, valueTypeList_p7_t1, filterList_p7_t1, mergeList, poivtList,
		limitNump8_t1, sortMap_p8_t1, selectList, titleList_p6_t1)

	val tableList = List(p2_t1, p3_t1, p4_t1, p5_t1, p5_t2, p6_t1, p6_t2, p7_t1, p8_t1)
//	val tableList = List(p4_t1)
	tableList.foreach { x =>
		generatePPTData().generateData(x, aggreagteDF)
		println(x.tableIndex + "======== 生成完成")
	}
}

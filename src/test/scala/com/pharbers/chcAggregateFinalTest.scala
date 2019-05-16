package com.pharbers

import com.pharbers.aggregate.common.phFactory
import com.pharbers.aggregate.moudle.pptInputData
import com.pharbers.aggregate.ppt.{aggregateData, phCommand}
import com.pharbers.aggregate.util.saveDF2mongo
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import org.apache.spark.sql.functions._

object chcAggregateFinalTest extends App {

	import com.pharbers.data.util._
	import com.pharbers.data.conversion._
	import com.pharbers.data.util.ParquetLocation._
	import com.pharbers.data.util.sparkDriver.ss.implicits._

	//	val chcFile1 = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
	//	val chcFile2 = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"
	//
	//	val piCvs = ProductImsConversion()
	//	val chcCvs = CHCConversion()
	//	//	val chcDF = (CSV2DF(chcFile1) unionByName CSV2DF(chcFile2)).distinct() // 8728
	//	val chcDF = CSV2DF(chcFile2)
	//	val chcDFCount = chcDF.count()
	//	val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
	//
	//	val productImsDIS = piCvs.toDIS(MapArgs(Map(
	//		"productImsERD" -> DFArgs(Mongo2DF(PROD_IMS_LOCATION.split("/").last))
	//		, "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
	//		, "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
	//		, "productDevERD" -> DFArgs(Mongo2DF("prod_dev9"))
	//	))).getAs[DFArgs]("productImsDIS")
	//
	//	val chcDIS = chcCvs.toDIS(MapArgs(Map(
	//		"chcERD" -> DFArgs(Mongo2DF(CHC_LOCATION.split("/").last))
	//		, "dateERD" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
	//		, "cityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
	//		, "productDIS" -> DFArgs(productImsDIS)
	//	))).getAs[DFArgs]("chcDIS")
	//
	//	val aggreagteDF = aggregateData().getAggregate(chcDIS)
	//
	//	//	aggreagteDF.coalesce(1).write
	//	//		.format("csv")
	//	//		.option("encoding", "UTF-8")
	//	//		.option("header", value = true)
	//	//		.option("delimiter", "#")
	//	//		.save("/test/chcAggResult")
	//
	//	saveDF2mongo().saveDF(aggreagteDF, "aggregateData")


	val aggreagteDF = Mongo2DF("aggregateData")

	val otherTag_normal = "normal"
	val otherTag_other = "other"
	val otherTag_noOther = "noOther"

	val sortOrderDesc = "desc"
	val sortOrderAsc = "asc"

	val dataList = List("2017Q4YTD", "2018Q4YTD")
	val valueTypeList = List("sales", "share")
	val selectList = List("key")
	val titleList = List("", "2017Q4YTD", "2018Q4YTD", "", "")
	val limitNum_p2_t1 = 7
	val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
	val keyProdListNomal: List[String] = List()

	//全国 p4_t1
	val dataList_na_p4_t1 = List("2017Q4YTD", "2018Q4YTD")
	val valueTypeList_na_p4_t1 = List("share")
	val filterList_na_p4_t1: List[(String, List[String])] = List(
		("keyType", List("city")),
		("date", dataList_na_p4_t1),
		("valueType", valueTypeList_na_p4_t1)
	)
	val limitNum_na_p4_t1 = 5
	val titleList_na_p4_t1 = List("", "2017", "2018")
	val p4_t1_nationwide = pptInputData("p4_t1_nationwide", dataList_na_p4_t1, valueTypeList_na_p4_t1, filterList_na_p4_t1,
		limitNum_na_p4_t1, sortOrderDesc, otherTag_normal, selectList, titleList_na_p4_t1, factory_nomal, keyProdListNomal)

	//全国 p4_t2
	val dataList_na_p4_t2 = List("2017Q4YTD", "2018Q4YTD")
	val valueTypeList_na_p4_t2 = List("sales", "share")
	val filterList_na_p4_t2: List[(String, List[String])] = List(
		("keyType", List("oad")),
		("city", List("全国")),
		("date", dataList_na_p4_t2),
		("valueType", valueTypeList_na_p4_t2)
	)
	val limitNum_na_p4_t2 = 7
	val titleList_na_p4_t2 = List("", "2017Q4YTD", "2018Q4YTD", "", "")
	val p4_t2_nationwide = pptInputData("p4_t2_nationwide", dataList_na_p4_t2, valueTypeList_na_p4_t2, filterList_na_p4_t2,
		limitNum_na_p4_t2, sortOrderAsc, otherTag_other, selectList, titleList_na_p4_t2, factory_nomal, keyProdListNomal)

	// 全国，p5_t1
	val dataList_p5_t1_nationwide = List("2018Q4YTD")
	val valueTypeList_p5_t1_nationwide = List("share", "growth", "sales")
	val filterList_p5_t1_nationwide = List(
		("keyType", List("corp")),
		("city", List("全国")),
		("date", dataList_p5_t1_nationwide),
		("valueType", valueTypeList_p5_t1_nationwide)
	)
	val titleList_p5_t1 = List("", "份额", "增长", "Size")
	val limitNum_p5_t1_nationwide = 16
	val p5_t1_nationwide = pptInputData("p5_t1_nationwide", dataList_p5_t1_nationwide, valueTypeList_p5_t1_nationwide,
		filterList_p5_t1_nationwide, limitNum_p5_t1_nationwide, sortOrderDesc, otherTag_normal, selectList,
		titleList_p5_t1, factory_nomal, keyProdListNomal)


	//全国，p6_t1_nationwide
	val titleList_p6_t1_nationwide = List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3",
		"2018Q4", "", "", "", "", "", "", "", "")
	val limitNum_p6_t1_nationwide = 9
	val valueTypeList_p6_t1_nationwide = List("sales", "share")
	val dataList_p6_t1_nationwide = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val filterList_p6_t1_nationwide = List(
		("keyType", List("mole")),
		("city", List("全国")),
		("date", dataList_p6_t1_nationwide),
		("valueType", valueTypeList_p6_t1_nationwide)
	)
	val p6_t1_nationwide = pptInputData("p6_t1_nationwide", dataList_p6_t1_nationwide, valueTypeList_p6_t1_nationwide,
		filterList_p6_t1_nationwide, limitNum_p6_t1_nationwide,
		sortOrderAsc, otherTag_noOther, selectList, titleList_p6_t1_nationwide, factory_nomal, keyProdListNomal)

	//全国，p7_t1_nationwide
	val dataList_p7_t1_nationwide = List("2018Q4YTD")
	val valueTypeList_p7_t1_nationwide = List("sales", "growth", "share", "shareGrowth", "EI")
	val filterList_p7_t1_nationwide: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p7_t1_nationwide),
		("valueType", valueTypeList_p7_t1_nationwide)
	)
	val limitNum_p7_t1_nationwide = 15
	val selectList_p7_t1_nationwide = List("key")
	val titleList_p7_t1_nationwide = List("市场排名", "重点产品", "sales", "growth", "share", "shareGrowth", "EI")
	val poivtList_p7_t1_nationwide = List("key")
	val p7_t1_nationwide = pptInputData("p7_t1_nationwide", dataList_p7_t1_nationwide, valueTypeList_p7_t1_nationwide,
		filterList_p7_t1_nationwide, limitNum_p7_t1_nationwide, sortOrderDesc, otherTag_normal, selectList_p7_t1_nationwide,
		titleList_p7_t1_nationwide, factory_nomal, keyProdListNomal)

	//全国，p8_t1_nationwide
	val dataList_p8_t1_nationwide = List("2017Q4YTD", "2018Q4YTD")
	val valueTypeList_p8_t1_nationwide = List("sales", "moleShare")
	val filterList_p8_t1_nationwide: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p8_t1_nationwide),
		("valueType", valueTypeList_p8_t1_nationwide),
		("mole_name", List("二甲双胍"))
	)
	val titleList_p8_t1_nationwide = List("", "2017Q4YTD", "2018Q4YTD", "", "")
	val limitNum_p8_t1_nationwide = 8
	val mergeList_p8_t1_nationwide = List("date", "valueType")
	val poivtList_p8_t1_nationwide = List("key")
	val selectList_p8_t1_nationwide = List("key")
	val keyProdList_p8 = List("格华止")
	val p8_t1_nationwide = pptInputData("p8_t1_nationwide", dataList_p8_t1_nationwide, valueTypeList_p8_t1_nationwide,
		filterList_p8_t1_nationwide, limitNum_p8_t1_nationwide, sortOrderAsc, otherTag_noOther,
		selectList_p8_t1_nationwide, titleList_p8_t1_nationwide, factory_nomal, keyProdList_p8)

	//全国，p8_t2_nationwide
	val dataList_p8_t2_nationwide = List("2018Q4YTD")
	val valueTypeList_p8_t2_nationwide = List("sales", "growth", "moleShare", "moleShareGrowth", "moleEI")
	val filterList_p8_t2_nationwide: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p8_t2_nationwide),
		("valueType", valueTypeList_p8_t2_nationwide),
		("mole_name", List("二甲双胍"))
	)
	val limitNum_p8_t2_nationwide = 9
	val selectList_p8_t2_nationwide = List("key", "corp_name")
	val titleList_p8_t2_nationwide = List("市场排名", "重点产品", "公司", "sales", "growth", "share", "shareGrowth", "EI")
	val poivtList_p8_t2_nationwide = List("key", "corp_name")
	val mergeList_p8_t2_nationwide = List("date", "valueType")
	val p8_t2_nationwide = pptInputData("p8_t2_nationwide", dataList_p8_t2_nationwide, valueTypeList_p8_t2_nationwide,
		filterList_p8_t2_nationwide, limitNum_p8_t2_nationwide, sortOrderDesc, otherTag_normal,
		selectList_p8_t2_nationwide, titleList_p8_t2_nationwide, factory_nomal, keyProdList_p8)

	val tableList = List(p4_t1_nationwide, p4_t2_nationwide, p5_t1_nationwide, p6_t1_nationwide, p7_t1_nationwide,
		p8_t1_nationwide, p8_t2_nationwide)
//		val tableList = List(p6_t1_nationwide)
	tableList.foreach { x =>
		phFactory.getInstance(x.factory).asInstanceOf[phCommand].exec(x, aggreagteDF)
		println(x.tableIndex + "======== 生成完成")
	}

	//	val cityList = List("上海市", "北京市", "广州市", "南京市", "杭州市")
	////	val cityList = List("杭州市")
	//	cityList.foreach { city =>
	//		cityPPT().getCityList(city).foreach { x =>
	//			phFactory.getInstance(x.factory).asInstanceOf[phCommand].exec(x, aggreagteDF)
	//			println(x.tableIndex + "======== 生成完成")
	//		}
	//	}
}

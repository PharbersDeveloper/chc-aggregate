package com.pharbers

import com.pharbers.aggregate.common.phFactory
import com.pharbers.aggregate.moudle.{caCityPPT, cityPPT, pptInputData}
import com.pharbers.aggregate.ppt.{aggregateData, phCommand}
import com.pharbers.aggregate.util.saveDF2mongo
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object caMarket extends App {

	import com.pharbers.data.util._
	import com.pharbers.data.conversion._
	import com.pharbers.data.util.ParquetLocation._
	import com.pharbers.data.util.sparkDriver.ss.implicits._

//	//	val chcFile1 = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
//	//	val chcFile2 = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"
//	val chcFile3 = "/test/CHC_CA_7cities_Delivery.csv"
//	val piCvs = ProductImsConversion()
//	val chcCvs = CHCConversion()
//	//	val chcDF = (CSV2DF(chcFile1) unionByName CSV2DF(chcFile2)).distinct() // 8728
//	val chcDF = CSV2DF(chcFile3)
//	val chcDFCount = chcDF.count()
//	val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
//
//	val productImsDIS = piCvs.toDIS(MapArgs(Map(
//		"productImsERD" -> DFArgs(Mongo2DF(PROD_IMS_LOCATION.split("/").last))
////		, "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
////		, "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
//		, "productDevERD" -> DFArgs(Mongo2DF("prod_dev9"))
//	))).getAs[DFArgs]("productImsDIS")
//	productImsDIS.show(false)
//	val chcERD = chcCvs.toERD(MapArgs(Map(
//		"chcDF" -> DFArgs(chcDF)
//		, "dateDF" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
//		, "cityDF" -> DFArgs(cityDF)
//		, "productDIS" -> DFArgs(productImsDIS)
//		, "addCHCProdFunc" -> SingleArgFuncArgs { df: DataFrame =>
//			ProductDevConversion().toERD(MapArgs(Map(
//				"chcDF" -> DFArgs(df)
//			))).getAs[DFArgs]("productDevERD")
//		}
//	))).getAs[DFArgs]("chcERD")
//
//	val chcDIS = chcCvs.toDIS(MapArgs(Map(
//		//			"chcERD" -> DFArgs(Mongo2DF(CHC_LOCATION.split("/").last))
//		"chcERD" -> DFArgs(chcERD)
//		, "dateERD" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
//		, "cityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
//		, "productDIS" -> DFArgs(productImsDIS)
//	))).getAs[DFArgs]("chcDIS")
//
//	val aggreagteDFTemp = aggregateData().getAggregate(chcDIS.na.fill(""))
//
//	//	//	aggreagteDF.coalesce(1).write
//	//	//		.format("csv")
//	//	//		.option("encoding", "UTF-8")
//	//	//		.option("header", value = true)
//	//	//		.option("delimiter", "#")
//	//	//		.save("/test/chcAggResult")
//	//
//	saveDF2mongo().saveDF(aggreagteDFTemp, "aggregateCaData")


	val aggreagteDF = Mongo2DF("aggregateCaData")

	val otherTag_normal = "normal"
	val otherTag_other = "other"
	val otherTag_noOther = "noOther"

	val sortOrderDesc = "desc"
	val sortOrderAsc = "asc"

	val selectList = List("key")
	val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
	val keyProdListNomal: List[String] = List()

	//全国 p4_t1
	val dataList_na_p4_t1 = List("2018Q4YTD")
	val valueTypeList_na_p4_t1 = List("share")
	val filterList_na_p4_t1: List[(String, List[String])] = List(
		("keyType", List("city")),
		("date", dataList_na_p4_t1),
		("valueType", valueTypeList_na_p4_t1)
	)
	val limitNum_na_p4_t1 = 7
	val titleList_na_p4_t1 = List("", "2018")
	val p4_t1_nationwide = pptInputData("p4_t1_nationwide", dataList_na_p4_t1, valueTypeList_na_p4_t1, filterList_na_p4_t1,
		limitNum_na_p4_t1, sortOrderDesc, otherTag_normal, selectList, titleList_na_p4_t1, factory_nomal, keyProdListNomal)

	//全国 p5_t1
	val dataList_na_p5_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val valueTypeList_na_p5_t1 = List("sales", "share")
	val filterList_na_p5_t1: List[(String, List[String])] = List(
		("keyType", List("mole")),
		("city", List("全国")),
		("date", dataList_na_p5_t1),
		("valueType", valueTypeList_na_p5_t1)
	)
	val limitNum_na_p5_t1 = 9
	val titleList_na_p5_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4", "", "", "", "")
	val p5_t1_nationwide = pptInputData("p5_t1_nationwide", dataList_na_p5_t1, valueTypeList_na_p5_t1, filterList_na_p5_t1,
		limitNum_na_p5_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_na_p5_t1, factory_nomal, keyProdListNomal)

	// 全国，p6_t1
	val dataList_p6_t1_nationwide = List("2018Q4YTD")
	val valueTypeList_p6_t1_nationwide = List("share", "growth", "sales")
	val filterList_p6_t1_nationwide = List(
		("keyType", List("corp")),
		("city", List("全国")),
		("date", dataList_p6_t1_nationwide),
		("valueType", valueTypeList_p6_t1_nationwide)
	)
	val titleList_p6_t1 = List("", "份额", "增长", "Size")
	val limitNum_p6_t1_nationwide = 15
	val p6_t1_nationwide = pptInputData("p6_t1_nationwide", dataList_p6_t1_nationwide, valueTypeList_p6_t1_nationwide,
		filterList_p6_t1_nationwide, limitNum_p6_t1_nationwide, sortOrderDesc, otherTag_normal, selectList,
		titleList_p6_t1, factory_nomal, keyProdListNomal)

	//全国，p7_t1_nationwide
	val titleList_p7_t1_nationwide = List("", "2018Q4YTD", "")
	val limitNum_p7_t1_nationwide = 12
	val valueTypeList_p7_t1_nationwide = List("sales", "share")
	val dataList_p7_t1_nationwide = List("2018Q4YTD")
	val filterList_p7_t1_nationwide = List(
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p6_t1_nationwide),
		("valueType", valueTypeList_p6_t1_nationwide)
	)
	val p7_t1_nationwide = pptInputData("p7_t1_nationwide", dataList_p7_t1_nationwide, valueTypeList_p7_t1_nationwide,
		filterList_p7_t1_nationwide, limitNum_p7_t1_nationwide,
		sortOrderAsc, otherTag_noOther, selectList, titleList_p7_t1_nationwide, factory_nomal, keyProdListNomal)

	//全国，p7_t2_nationwide
	val dataList_p7_t2_nationwide = List("2018Q4YTD")
	val valueTypeList_p7_t2_nationwide = List("sales", "growth", "share", "shareGrowth", "EI")
	val filterList_p7_t2_nationwide: List[(String, List[String])] = List(
		("market", List("钙补充剂市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p7_t2_nationwide),
		("valueType", valueTypeList_p7_t2_nationwide)
	)
	val limitNum_p7_t2_nationwide = 13
	val selectList_p7_t2_nationwide = List("rank", "key", "mole_name", "corp_name")
	val titleList_p7_t2_nationwide = List("市场排名", "重点产品", "分子", "公司","销量(Mn)", "销量同比增长", "marketShare(%)", "marketShare同比增长", "EI")
	val poivtList_p7_t2_nationwide = List("key")
	val p7_t2_nationwide = pptInputData("p7_t2_nationwide", dataList_p7_t2_nationwide, valueTypeList_p7_t2_nationwide,
		filterList_p7_t2_nationwide, limitNum_p7_t2_nationwide, sortOrderDesc, otherTag_normal, selectList_p7_t2_nationwide,
		titleList_p7_t2_nationwide, factory_nomal, keyProdListNomal)

	//全国，p8_t1_nationwide
	val dataList_p8_t1_nationwide = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val valueTypeList_p8_t1_nationwide = List("sales")
	val filterList_p8_t1_nationwide: List[(String, List[String])] = List(
		("market", List("钙补充剂市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p8_t1_nationwide),
		("valueType", valueTypeList_p8_t1_nationwide)
	)
	val titleList_p8_t1_nationwide = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val limitNum_p8_t1_nationwide = 12
	val selectList_p8_t1_nationwide = List("key")
	val p8_t1_nationwide = pptInputData("p8_t1_nationwide", dataList_p8_t1_nationwide, valueTypeList_p8_t1_nationwide,
		filterList_p8_t1_nationwide, limitNum_p8_t1_nationwide, sortOrderAsc, otherTag_normal,
		selectList_p8_t1_nationwide, titleList_p8_t1_nationwide, factory_nomal, keyProdListNomal)

	//全国，p9_t1_nationwide
	val dataList_p9_t1_nationwide = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val valueTypeList_p9_t1_nationwide = List("sales", "share")
	val filterList_p9_t1_nationwide: List[(String, List[String])] = List(
		("market", List("钙补充剂市场")),
		("keyType", List("prod")),
		("city", List("全国")),
		("date", dataList_p9_t1_nationwide),
		("valueType", valueTypeList_p9_t1_nationwide)
	)
	val limitNum_p9_t1_nationwide = 13
	val selectList_p9_t1_nationwide = List("key")
	val titleList_p9_t1_nationwide = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
	val p9_t1_nationwide = pptInputData("p9_t1_nationwide", dataList_p9_t1_nationwide, valueTypeList_p9_t1_nationwide,
		filterList_p9_t1_nationwide, limitNum_p9_t1_nationwide, sortOrderAsc, otherTag_noOther,
		selectList_p9_t1_nationwide, titleList_p9_t1_nationwide, factory_nomal, keyProdListNomal)

		val tableList = List(p4_t1_nationwide, p5_t1_nationwide, p6_t1_nationwide, p7_t1_nationwide, p7_t2_nationwide,
			p8_t1_nationwide, p9_t1_nationwide)
//	val tableList = List(p4_t1_nationwide, p5_t1_nationwide, p6_t1_nationwide)
	tableList.foreach { x =>
		phFactory.getInstance(x.factory).asInstanceOf[phCommand].exec(x, aggreagteDF)
		println(x.tableIndex + "======== 生成完成")
	}

	val cityList = List("上海市", "北京市", "广州市", "南京市", "杭州市", "宁波市", "苏州市")
//		val cityList = List("宁波市", "苏州市")
	cityList.foreach { city =>
		caCityPPT().getCityList(city).foreach { x =>
			phFactory.getInstance(x.factory).asInstanceOf[phCommand].exec(x, aggreagteDF)
			println(x.tableIndex + "======== 生成完成")
		}
	}
}

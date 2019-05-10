//package com.pharbers
//
//import com.pharbers.ahcaggregate.common.phFactory
//import com.pharbers.ahcaggregate.moudle.pptInputData
//import com.pharbers.ahcaggregate.ppt.{aggregateForPPT, generatePPTData}
//import com.pharbers.spark.util.readCsv
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.functions._
//
//object pptTest extends App {
//	val sprakDriver = phFactory.getSparkInstance()
//
//	import sprakDriver.conn_instance
//
//	val chcDF = sprakDriver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
//		.select("city", "Date", "OAD类别", "Molecule_Desc", "分子", "Prod_Desc", "MNF_Desc", "Sales")
//		.na.fill("")
//		.withColumn("Sales", col("Sales").cast(DoubleType))
//		.withColumn("market", lit("降糖药市场"))
//	val resultDF = aggregateForPPT().getAggregate(chcDF)
//
//
//	val dataList = List("2017Q3YTD", "2018Q3YTD")
//	val valueTypeList = List("sales", "share")
//	val filterList: List[(String, List[String])] = List(
//		("market", List("降糖药市场")),
//		("keyType", List("oad")),
//		("key", List("其他", "GLP-1激动剂", "DPP4抑制剂", "格列酮类", "格列奈类", "磺脲类", "双胍类", "糖苷类")),
//		("city", List("北京")),
//		("date", dataList),
//		("valueType", valueTypeList)
//	)
//	val mergeList = List("date", "valueType")
//	val poivtList = List("key")
//	val sortList = List(col("2018Q3YTD"))
//	val selectList = List("key")
//	val titleList = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD", "2018Q3YTD"))
//
//	val p1_t1 = pptInputData("p2_t1", dataList, valueTypeList, filterList, mergeList, poivtList, sortList, selectList, titleList)
//	val p2_t1 = pptInputData("p3_t1", List("2018Q3YTD"), List("sales", "share", "growth"), List(("keyType", List("mnf")),
//		("city", List("北京")), ("date", List("2018Q3YTD")), ("valueType", List("sales", "share", "growth"))), mergeList,
//		poivtList, sortList, selectList, List(List("", "份额", "增长", "Size"))
//	)
//
//	val dataList_p3 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3")
//	val filterList_p3 = List(
//		("keyType", List("mole")),
//		("city", List("北京")),
//		("date", dataList_p3),
//		("valueType", valueTypeList)
//	)
//	val titleList_p2 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "", "", "", "", "", "", ""))
//	val p3_t1 = pptInputData("p4_t1", dataList_p3, valueTypeList, filterList_p3, mergeList, poivtList, sortList, selectList, titleList)
//
//	val filterList_p4_t1: List[(String, List[String])] = List(
//		("market", List("降糖药市场")),
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList),
//		("valueType", valueTypeList)
//	)
//	val titleList_p4_t1 = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD2", "2018Q3YTD3"))
//	val p4_t1 = pptInputData("p5_t1", dataList, valueTypeList, filterList_p4_t1, mergeList, poivtList, sortList, selectList, titleList)
//
//	val dataList_p4_t2 = List("2018Q3YTD")
//	val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
//	val filterList_p4_t2: List[(String, List[String])] = List(
//		("market", List("降糖药市场")),
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList_p4_t2),
//		("valueType", valueTypeList_p4_t2)
//	)
//	val titleList_p4_t2 = List(List("重点产品", "sales", "growth", "share", "shareGrowth", "EI"))
//	val p4_t2 = pptInputData("p5_t2", dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2, mergeList, poivtList, sortList, selectList, titleList_p4_t2)
//
//	val filterList_p5_1: List[(String, List[String])] = List(
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList),
//		("valueType", valueTypeList)
//	)
//	val titleList_p5_t1 = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD2", "2018Q3YTD3"))
//	val p5_t1 = pptInputData("p6_t1", dataList, valueTypeList, filterList, mergeList, poivtList, sortList, selectList, titleList_p5_t1)
//
//	val dataList_p5_t2 = List("2018Q3")
//	val filterList_p5_t2: List[(String, List[String])] = List(
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList_p5_t2),
//		("valueType", valueTypeList_p4_t2)
//	)
//	val p5_t2 = pptInputData("p6_t2", dataList, valueTypeList_p4_t2, filterList_p5_t2, mergeList, poivtList, sortList, selectList, titleList_p4_t2)
//
//	val dataList_p6_t1 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3")
//	val valueTypeList_p6_t1 = List("sales")
//	val filterList_p6_t1: List[(String, List[String])] = List(
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList_p6_t1),
//		("valueType", valueTypeList_p6_t1)
//	)
//	val titleList_p6_t1 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3"))
//	val p6_t1 = pptInputData("p7_t1", dataList_p6_t1, valueTypeList_p6_t1, filterList_p6_t1, mergeList, poivtList, sortList, selectList, titleList_p6_t1)
//
//	val valueTypeList_p7_t1 = List("sales", "share")
//	val filterList_p7_t1: List[(String, List[String])] = List(
//		("keyType", List("prod")),
//		("city", List("北京")),
//		("date", dataList_p6_t1),
//		("valueType", valueTypeList_p7_t1)
//	)
//	val p7_t1 = pptInputData("p8_t1", dataList_p6_t1, valueTypeList_p7_t1, filterList_p7_t1, mergeList, poivtList, sortList, selectList, titleList_p6_t1)
//
//	val tableList = List(p6_t1, p7_t1)
//	tableList.foreach{x =>
//		generatePPTData().generateData(x, resultDF)
//		println(x.tableIndex + "======== 生成完成")
//	}
//}

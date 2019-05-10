package com.pharbers.aggregate.moudle

import org.apache.spark.sql.functions.col

case class cityPPT() {
	def getCityList(cityName: String): List[pptInputData] ={
		val strMap = Map("北京市" -> "beijing", "南京市" -> "nanjing", "上海市" -> "shagnhai", "广州市" -> "guangzhou",
			"杭州市" -> "hangzhou")
		val cityStr = strMap(cityName)
		val dataList = List("2017Q4YTD", "2018Q4YTD")
		val valueTypeList = List("sales", "share")
		val filterList: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("oad")),
			("city", List(cityName)),
			("date", dataList),
			("valueType", valueTypeList)
		)
		val mergeList = List("date", "valueType")
		val poivtList = List("key")
		val sortMap = Map("asc" -> col("2018Q4YTDsales"))
		val selectList = List("key")
		val titleList = List(List("", "2017Q4YTD", "2018Q4YTD", "", ""))
		val limitNum_p2_t1 = 7
		val sortStr_nomal = "nomal"
		val sortStr_other = "other"
		val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
		val factory_rank = "com.pharbers.aggregate.ppt.generateDataWithIndex"
		val p2_t1 = pptInputData("p2_t1_" + cityStr, dataList, valueTypeList, filterList, mergeList, poivtList, limitNum_p2_t1, sortMap,
			sortStr_nomal, selectList, titleList, factory_nomal)

		val sortMap_p2_t1 = Map("desc" -> col("2018Q4YTDsales"))
		val filterList_p3_t1 = List(("keyType", List("corp")),
			("city", List(cityName)),
			("date", List("2018Q4YTD")),
			("valueType", List("sales", "share", "growth"))
		)
		val limitNum_p3_t1 = 15
		val p3_t1 = pptInputData("p3_t1_" + cityStr, List("2018Q4YTD"), List("share", "growth", "sales"), filterList_p3_t1, mergeList,
			poivtList, limitNum_p3_t1, sortMap_p2_t1, sortStr_nomal, selectList, List(List("", "份额", "增长", "Size")), factory_nomal
		)

		val dataList_p3 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val filterList_p3 = List(
			("keyType", List("mole")),
			("city", List(cityName)),
			("date", dataList_p3),
			("valueType", valueTypeList)
		)

		val titleList_p2 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "2018Q4", "", "", "", "", "", "", "", ""))
		val sortMap_p3_t1 = Map("asc" -> col("2018Q4sales"))
		val limitNum_p4_t1 = 9
		val p4_t1 = pptInputData("p4_t1_" + cityStr, dataList_p3, valueTypeList, filterList_p3, mergeList, poivtList, limitNum_p4_t1,
			sortMap_p3_t1, sortStr_other, selectList, titleList_p2, factory_nomal)

		val filterList_p4_t1: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList),
			("valueType", valueTypeList)
		)
		val sortMap_p4_t1 = Map("asc" -> col("2018Q4YTDsales"))
		val limitNum_p5_t1 = 10
		val p5_t1 = pptInputData("p5_t1_" + cityStr, dataList, valueTypeList, filterList_p4_t1, mergeList, poivtList, limitNum_p5_t1,
			sortMap_p4_t1, sortStr_other, selectList, titleList, factory_nomal)

		val dataList_p4_t2 = List("2018Q4YTD")
		val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
		val filterList_p4_t2: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t2),
			("valueType", valueTypeList_p4_t2)
		)
		val limitNum_p5_t2 = 11
		val selectList_p4_t2 = List("key", "mole_name", "corp_name")
		val titleList_p4_t2 = List(List("市场排名", "重点产品", "分子", "公司", "sales", "growth", "share", "shareGrowth", "EI"))
		val poivtList_p4_t2 = List("key", "mole_name", "corp_name")
		val sortMap_p4_t2 = Map("desc" -> col("2018Q4YTDsales"))
		val p5_t2 = pptInputData("p5_t2_" + cityStr, dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2, mergeList, poivtList_p4_t2,
			limitNum_p5_t2, sortMap_p4_t2, sortStr_nomal, selectList_p4_t2, titleList_p4_t2, factory_rank)

		val filterList_p5_1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList),
			("valueType", valueTypeList),
			("mole_name", List("二甲双胍"))
		)
		val titleList_p5_t1 = List(List("", "2017Q4YTD", "2018Q4YTD", "", ""))
		val sortMap_p5_t1 = Map("asc" -> col("2018Q4YTDsales"))
		val limitNum_p6_t1 = 11
		val p6_t1 = pptInputData("p6_t1_" + cityStr, dataList, valueTypeList, filterList_p5_1, mergeList, poivtList, limitNum_p6_t1,
			sortMap_p5_t1, sortStr_other, selectList, titleList_p5_t1, factory_nomal)

		val dataList_p5_t2 = List("2018Q4YTD")
		val valueTypeList_p6_t2 = List("sales", "growth", "moleShare", "moleShareGrowth", "moleEI")
		val filterList_p5_t2: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p5_t2),
			("valueType", valueTypeList_p6_t2),
			("mole_name", List("二甲双胍"))
		)
		val selectList_p5_t2 = List("key", "corp_name")
		val sortMap_p6_t2 = Map("desc" -> col("2018Q4YTDsales"))
		val titleList_p6_t2 = List(List("市场排名", "重点产品", "公司", "sales", "growth", "share", "shareGrowth", "EI"))
		val limitNum_p6_t2 = 9
		val mergeList_p6_t2 = List("date", "valueType")
		val p6_t2 = pptInputData("p6_t2_" + cityStr, dataList_p5_t2, valueTypeList_p6_t2, filterList_p5_t2, mergeList_p6_t2, selectList_p5_t2,
			limitNum_p6_t2, sortMap_p6_t2, sortStr_nomal, selectList_p5_t2, titleList_p6_t2, factory_rank)

		val dataList_p6_t1 = List("2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p6_t1 = List("sales")
		val filterList_p6_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p6_t1),
			("valueType", valueTypeList_p6_t1),
			("mole_name", List("二甲双胍"))
		)
		val titleList_p6_t1 = List(List("", "2017Q1", "2017Q2", "2017Q3", "2017Q4", "2018Q1", "2018Q2", "2018Q3", "2018Q4"))
		val sortMap_p7_t1 = Map("asc" -> col("2018Q4sales"))
		val limitNum_p7_t1 = 10
		val p7_t1 = pptInputData("p7_t1_" + cityStr, dataList_p6_t1, valueTypeList_p6_t1, filterList_p6_t1, mergeList, poivtList,
			limitNum_p7_t1, sortMap_p7_t1, sortStr_nomal, selectList, titleList_p6_t1, factory_nomal)

		val valueTypeList_p7_t1 = List("sales", "moleShare")
		val filterList_p7_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p6_t1),
			("valueType", valueTypeList_p7_t1),
			("mole_name", List("二甲双胍"))
		)
		val sortMap_p8_t1 = Map("asc" -> col("2018Q4sales"))
		val limitNump8_t1 = 10

		val p8_t1 = pptInputData("p8_t1_" + cityStr, dataList_p6_t1, valueTypeList_p7_t1, filterList_p7_t1, mergeList, poivtList,
			limitNump8_t1, sortMap_p8_t1, sortStr_other, selectList, titleList_p6_t1, factory_nomal)
//		List(p2_t1, p3_t1, p4_t1, p5_t1, p5_t2, p6_t1, p6_t2, p7_t1, p8_t1)
		List(p6_t2)
	}
}

package com.pharbers.aggregate.moudle

case class cityPPT() {
	def getCityList(cityName: String): List[pptInputData] = {
		val strMap = Map("北京市" -> "beijing", "南京市" -> "nanjing", "上海市" -> "shagnhai", "广州市" -> "guangzhou",
			"杭州市" -> "hangzhou")
		val cityStr = strMap(cityName)
		val otherTag_normal = "normal"
		val otherTag_other = "other"
		val otherTag_noOther = "noOther"

		val sortOrderDesc = "desc"
		val sortOrderAsc = "asc"

		val dataList_p2_t1 = List("2017Q4YTD", "2018Q4YTD")
		val valueTypeList = List("sales", "share")
		val filterList: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("oad")),
			("city", List(cityName)),
			("date", dataList_p2_t1),
			("valueType", valueTypeList)
		)
		val selectList = List("key")
		val titleList_p2_t1 = List("", "2017Q4YTD", "2018Q4YTD", "", "")
		val limitNum_p2_t1 = 7
		val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
		val keyProdListNomal: List[String] = List()
		val p2_t1 = pptInputData("p2_t1_" + cityStr, dataList_p2_t1, valueTypeList, filterList, limitNum_p2_t1,
			sortOrderAsc, otherTag_other, selectList, titleList_p2_t1, factory_nomal, keyProdListNomal)

		val dataList_p3_t1 = List("2018Q4YTD")
		val valueTyepList_p3_t1 = List("share", "growth", "sales")
		val filterList_p3_t1 = List(("keyType", List("corp")),
			("city", List(cityName)),
			("date", dataList_p3_t1),
			("valueType", valueTyepList_p3_t1)
		)
		val limitNum_p3_t1 = 10
		val titleList_p3_t1 = List("", "份额", "增长", "Size")
		val p3_t1 = pptInputData("p3_t1_" + cityStr, dataList_p3_t1, valueTyepList_p3_t1, filterList_p3_t1,
			limitNum_p3_t1, sortOrderDesc, otherTag_normal, selectList, titleList_p3_t1, factory_nomal, keyProdListNomal)

		val dataList_p4_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val filterListp4_t1 = List(
			("keyType", List("mole")),
			("city", List(cityName)),
			("date", dataList_p4_t1),
			("valueType", valueTypeList)
		)
		val titleList_p4_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val limitNum_p4_t1 = 7
		val p4_t1 = pptInputData("p4_t1_" + cityStr, dataList_p4_t1, valueTypeList, filterListp4_t1,
			limitNum_p4_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_p4_t1, factory_nomal, keyProdListNomal)

		val dataList_p5_t1 = List("2018Q4YTD")
		val filterList_p5_t1: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p5_t1),
			("valueType", valueTypeList)
		)
		val limitNum_p5_t1 = 10
		val keyProdList_P5_1 = List("格华止")
		val titleList_p5_t1 = List("", "2018Q4YTD")
		val p5_t1 = pptInputData("p5_t1_" + cityStr, dataList_p5_t1, valueTypeList, filterList_p5_t1,
			limitNum_p5_t1, sortOrderAsc, otherTag_noOther,selectList, titleList_p5_t1, factory_nomal, keyProdList_P5_1)

		val dataList_p4_t2 = List("2018Q4YTD")
		val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
		val filterList_p4_t2: List[(String, List[String])] = List(
			("market", List("降糖药市场")),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t2),
			("valueType", valueTypeList_p4_t2)
		)
		val limitNum_p5_t2 = 10
		val selectList_p4_t2 = List("rank", "key", "mole_name", "corp_name")
		val keyProdList_P5_t2 = List("格华止")
		val titleList_p4_t2 = List("市场排名", "重点产品", "分子", "公司", "sales", "growth", "share", "shareGrowth", "EI")
		val p5_t2 = pptInputData("p5_t2_" + cityStr, dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2,
			limitNum_p5_t2, sortOrderDesc, otherTag_normal,selectList_p4_t2, titleList_p4_t2, factory_nomal, keyProdList_P5_t2)

		val dataList_p6_t1 = List("2017Q4YTD", "2018Q4YTD")
		val valueTypeList_6_1 = List("sales", "moleShare")
		val filterList_p5_1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p6_t1),
			("valueType", valueTypeList_6_1)
//			("mole_name", List("二甲双胍"))
		)
		val titleList_p6_t1 = List("", "2017Q4YTD", "2018Q4YTD", "", "")
		val limitNum_p6_t1 = 9
		val keyProdList_p6 = List("格华止")
		val p6_t1 = pptInputData("p6_t1_" + cityStr, dataList_p6_t1, valueTypeList_6_1, filterList_p5_1,
			limitNum_p6_t1, sortOrderAsc, otherTag_noOther,selectList, titleList_p6_t1, factory_nomal, keyProdList_p6)

		val dataList_p5_t2 = List("2018Q4YTD")
		val valueTypeList_p6_t2 = List("sales", "growth", "moleShare", "moleShareGrowth", "moleEI")
		val filterList_p5_t2: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p5_t2),
			("valueType", valueTypeList_p6_t2)
//			("mole_name", List("二甲双胍"))
		)
		val selectList_p5_t2 = List("rank", "key", "corp_name")
		val titleList_p6_t2 = List("市场排名", "重点产品", "公司", "sales", "growth", "share", "shareGrowth", "EI")
		val limitNum_p6_t2 = 11
		val p6_t2 = pptInputData("p6_t2_" + cityStr, dataList_p5_t2, valueTypeList_p6_t2, filterList_p5_t2,
			limitNum_p6_t2, sortOrderDesc, otherTag_normal, selectList_p5_t2, titleList_p6_t2, factory_nomal, keyProdList_p6)

		val dataList_p7_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p6_t1 = List("sales")
		val filterList_p6_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p7_t1),
			("valueType", valueTypeList_p6_t1)
//			("mole_name", List("二甲双胍"))
		)
		val titleList_p7_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val limitNum_p7_t1 = 9
		val keyProdList_p7 = List("格华止")
		val p7_t1 = pptInputData("p7_t1_" + cityStr, dataList_p7_t1, valueTypeList_p6_t1, filterList_p6_t1,
			limitNum_p7_t1, sortOrderAsc, otherTag_normal, selectList, titleList_p7_t1, factory_nomal, keyProdList_p7)

		val dataList_p8_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p8_t1 = List("sales", "moleShare")
		val filterList_p8_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p8_t1),
			("valueType", valueTypeList_p8_t1)
//			("mole_name", List("二甲双胍"))
		)
		val limitNump8_t1 = 9
		val keyProdList_p8 = List("格华止")
		val titleList_p8_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val p8_t1 = pptInputData("p8_t1_" + cityStr, dataList_p8_t1, valueTypeList_p8_t1, filterList_p8_t1,
			limitNump8_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_p8_t1, factory_nomal, keyProdList_p8)
//		List(p2_t1, p3_t1, p4_t1, p5_t1, p5_t2, p6_t1, p6_t2, p7_t1, p8_t1)
		List(p4_t1, p3_t1, p5_t1, p5_t2, p7_t1)
	}
}

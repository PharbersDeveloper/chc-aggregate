package com.pharbers.aggregate.moudle

case class caCityPPT() {
	def getCityList(cityName: String): List[pptInputData] = {
		val strMap = Map("北京市" -> "beijing", "南京市" -> "nanjing", "上海市" -> "shagnhai", "广州市" -> "guangzhou",
			"杭州市" -> "hangzhou", "宁波市" -> "ningbo", "苏州市" -> "suzhou")
		val cityStr = strMap(cityName)
		val otherTag_normal = "normal"
		val otherTag_other = "other"
		val otherTag_noOther = "noOther"

		val sortOrderDesc = "desc"
		val sortOrderAsc = "asc"

		val dataList_p2_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList = List("sales", "share")
		val filterList: List[(String, List[String])] = List(
			("market", List("钙补充剂市场")),
			("keyType", List("mole")),
			("city", List(cityName)),
			("date", dataList_p2_t1),
			("valueType", valueTypeList)
		)
		val selectList = List("key")
		val titleList_p2_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4", "", "", "", "")
		val limitNum_p2_t1 = 7
		val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
		val keyProdListNomal: List[String] = List()
		val p2_t1 = pptInputData("p2_t1_" + cityStr, dataList_p2_t1, valueTypeList, filterList, limitNum_p2_t1,
			sortOrderAsc, otherTag_normal, selectList, titleList_p2_t1, factory_nomal, keyProdListNomal)

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

		val dataList_p4_t1 = List("2018Q4YTD")
		val filterListp4_t1 = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t1),
			("valueType", valueTypeList)
		)
		val titleList_p4_t1 = List("", "2018Q4YTD")
		val limitNum_p4_t1 = 11
		val p4_t1 = pptInputData("p4_t1_" + cityStr, dataList_p4_t1, valueTypeList, filterListp4_t1,
			limitNum_p4_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_p4_t1, factory_nomal, keyProdListNomal)

		val dataList_p4_t2 = List("2018Q4YTD")
		val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
		val filterList_p4_t2: List[(String, List[String])] = List(
			("market", List("钙补充剂市场")),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t2),
			("valueType", valueTypeList_p4_t2)
		)
		val limitNum_p4_t2 = 11
		val selectList_p4_t2 = List("rank", "key", "mole_name", "corp_name")
		val titleList_p4_t2 = List("市场排名", "重点产品", "分子", "公司", "sales", "growth", "share", "shareGrowth", "EI")
		val p5_t2 = pptInputData("p4_t2_" + cityStr, dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2,
			limitNum_p4_t2, sortOrderDesc, otherTag_normal,selectList_p4_t2, titleList_p4_t2, factory_nomal, keyProdListNomal)

		val dataList_p5_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p5_t1 = List("sales")
		val filterList_p5_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p5_t1),
			("valueType", valueTypeList_p5_t1)
		)
		val titleList_p5_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val limitNum_p5_t1 = 11
		val p5_t1 = pptInputData("p5_t1_" + cityStr, dataList_p5_t1, valueTypeList_p5_t1, filterList_p5_t1,
			limitNum_p5_t1, sortOrderAsc, otherTag_normal, selectList, titleList_p5_t1, factory_nomal, keyProdListNomal)

		val dataList_p6_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p6_t1 = List("sales", "share")
		val filterList_p6_t1: List[(String, List[String])] = List(
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p6_t1),
			("valueType", valueTypeList_p6_t1)
		)
		val limitNum_p6_t1 = 11
		val titleList_p6_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val p6_t1 = pptInputData("p6_t1_" + cityStr, dataList_p6_t1, valueTypeList_p6_t1, filterList_p6_t1,
			limitNum_p6_t1, sortOrderAsc, otherTag_normal, selectList, titleList_p6_t1, factory_nomal, keyProdListNomal)

		List(p2_t1, p3_t1, p4_t1, p5_t1, p5_t2, p6_t1)
		//		List(p8_t1)
	}
}

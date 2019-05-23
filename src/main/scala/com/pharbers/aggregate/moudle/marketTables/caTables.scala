package com.pharbers.aggregate.moudle.marketTables

import com.pharbers.aggregate.moudle.pptInputData

case class caTables() {
	val market = "ca"
	val marketCN = "钙补充剂市场"
	val cityList = List("北京市", "南京市", "上海市", "广州市", "杭州市", "宁波市", "苏州市")
	val otherTag_normal = "normal"
	val otherTag_other = "other"
	val otherTag_noOther = "noOther"

	val sortOrderDesc = "desc"
	val sortOrderAsc = "asc"

	val selectList = List("key")
	val factory_nomal = "com.pharbers.aggregate.ppt.generatePPTData"
	val keyProdListNomal: List[String] = List()

	val strMap = Map("北京市" -> "beijing", "南京市" -> "nanjing", "上海市" -> "shagnhai", "广州市" -> "guangzhou",
		"杭州市" -> "hangzhou", "宁波市" -> "ningbo", "苏州市" -> "suzhou")

	def getCityList(cityName: String): List[pptInputData] = {
		val cityStr = strMap(cityName)

		val dataList_p2_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList = List("sales", "share")
		val filterList: List[(String, List[String])] = List(
			("market", List(marketCN)),
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
		val p2_t1 = pptInputData(market + "_" + "p2_t1_" + cityStr, dataList_p2_t1, valueTypeList, filterList, limitNum_p2_t1,
			sortOrderAsc, otherTag_normal, selectList, titleList_p2_t1, factory_nomal, keyProdListNomal)

		val dataList_p3_t1 = List("2018Q4YTD")
		val valueTyepList_p3_t1 = List("share", "growth", "sales")
		val filterList_p3_t1 = List(
			("market", List(marketCN)),
			("keyType", List("corp")),
			("city", List(cityName)),
			("date", dataList_p3_t1),
			("valueType", valueTyepList_p3_t1)
		)
		val limitNum_p3_t1 = 10
		val titleList_p3_t1 = List("", "份额", "增长", "Size")
		val p3_t1 = pptInputData(market + "_" + "p3_t1_" + cityStr, dataList_p3_t1, valueTyepList_p3_t1, filterList_p3_t1,
			limitNum_p3_t1, sortOrderDesc, otherTag_normal, selectList, titleList_p3_t1, factory_nomal, keyProdListNomal)

		val dataList_p4_t1 = List("2018Q4YTD")
		val filterListp4_t1 = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t1),
			("valueType", valueTypeList)
		)
		val titleList_p4_t1 = List("", "2018Q4YTD")
		val limitNum_p4_t1 = 11
		val p4_t1 = pptInputData(market + "_" + "p4_t1_" + cityStr, dataList_p4_t1, valueTypeList, filterListp4_t1,
			limitNum_p4_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_p4_t1, factory_nomal, keyProdListNomal)

		val dataList_p4_t2 = List("2018Q4YTD")
		val valueTypeList_p4_t2 = List("sales", "growth", "share", "shareGrowth", "EI")
		val filterList_p4_t2: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p4_t2),
			("valueType", valueTypeList_p4_t2)
		)
		val limitNum_p4_t2 = 11
		val selectList_p4_t2 = List("rank", "key", "mole_name", "corp_name")
		val titleList_p4_t2 = List("市场排名", "重点产品", "分子", "公司", "sales", "growth", "share", "shareGrowth", "EI")
		val p5_t2 = pptInputData(market + "_" + "p4_t2_" + cityStr, dataList_p4_t2, valueTypeList_p4_t2, filterList_p4_t2,
			limitNum_p4_t2, sortOrderDesc, otherTag_normal,selectList_p4_t2, titleList_p4_t2, factory_nomal, keyProdListNomal)

		val dataList_p5_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p5_t1 = List("sales")
		val filterList_p5_t1: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p5_t1),
			("valueType", valueTypeList_p5_t1)
		)
		val titleList_p5_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val limitNum_p5_t1 = 11
		val p5_t1 = pptInputData(market + "_" + "p5_t1_" + cityStr, dataList_p5_t1, valueTypeList_p5_t1, filterList_p5_t1,
			limitNum_p5_t1, sortOrderAsc, otherTag_normal, selectList, titleList_p5_t1, factory_nomal, keyProdListNomal)

		val dataList_p6_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p6_t1 = List("sales", "share")
		val filterList_p6_t1: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List(cityName)),
			("date", dataList_p6_t1),
			("valueType", valueTypeList_p6_t1)
		)
		val limitNum_p6_t1 = 11
		val titleList_p6_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val p6_t1 = pptInputData(market + "_" + "p6_t1_" + cityStr, dataList_p6_t1, valueTypeList_p6_t1, filterList_p6_t1,
			limitNum_p6_t1, sortOrderAsc, otherTag_normal, selectList, titleList_p6_t1, factory_nomal, keyProdListNomal)

		List(p2_t1, p3_t1, p4_t1, p5_t1, p5_t2, p6_t1)
		//		List(p8_t1)
	}

	def getNationwideList(): List[pptInputData] ={
		//全国 p4_t1
		val dataList_na_p4_t1 = List("2018Q4YTD")
		val valueTypeList_na_p4_t1 = List("share")
		val filterList_na_p4_t1: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("city")),
			("date", dataList_na_p4_t1),
			("valueType", valueTypeList_na_p4_t1)
		)
		val limitNum_na_p4_t1 = 7
		val titleList_na_p4_t1 = List("", "2018")
		val p4_t1_nationwide = pptInputData(market + "_" + "p4_t1_nationwide", dataList_na_p4_t1, valueTypeList_na_p4_t1, filterList_na_p4_t1,
			limitNum_na_p4_t1, sortOrderDesc, otherTag_normal, selectList, titleList_na_p4_t1, factory_nomal, keyProdListNomal)

		//全国 p5_t1
		val dataList_na_p5_t1 = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_na_p5_t1 = List("sales", "share")
		val filterList_na_p5_t1: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("mole")),
			("city", List("全国")),
			("date", dataList_na_p5_t1),
			("valueType", valueTypeList_na_p5_t1)
		)
		val limitNum_na_p5_t1 = 9
		val titleList_na_p5_t1 = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4", "", "", "", "")
		val p5_t1_nationwide = pptInputData(market + "_" + "p5_t1_nationwide", dataList_na_p5_t1, valueTypeList_na_p5_t1, filterList_na_p5_t1,
			limitNum_na_p5_t1, sortOrderAsc, otherTag_noOther, selectList, titleList_na_p5_t1, factory_nomal, keyProdListNomal)

		// 全国，p6_t1
		val dataList_p6_t1_nationwide = List("2018Q4YTD")
		val valueTypeList_p6_t1_nationwide = List("share", "growth", "sales")
		val filterList_p6_t1_nationwide = List(
			("market", List(marketCN)),
			("keyType", List("corp")),
			("city", List("全国")),
			("date", dataList_p6_t1_nationwide),
			("valueType", valueTypeList_p6_t1_nationwide)
		)
		val titleList_p6_t1 = List("", "份额", "增长", "Size")
		val limitNum_p6_t1_nationwide = 15
		val p6_t1_nationwide = pptInputData(market + "_" + "p6_t1_nationwide", dataList_p6_t1_nationwide, valueTypeList_p6_t1_nationwide,
			filterList_p6_t1_nationwide, limitNum_p6_t1_nationwide, sortOrderDesc, otherTag_normal, selectList,
			titleList_p6_t1, factory_nomal, keyProdListNomal)

		//全国，p7_t1_nationwide
		val titleList_p7_t1_nationwide = List("", "2018Q4YTD", "")
		val limitNum_p7_t1_nationwide = 12
		val valueTypeList_p7_t1_nationwide = List("sales", "share")
		val dataList_p7_t1_nationwide = List("2018Q4YTD")
		val filterList_p7_t1_nationwide = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List("全国")),
			("date", dataList_p6_t1_nationwide),
			("valueType", valueTypeList_p6_t1_nationwide)
		)
		val p7_t1_nationwide = pptInputData(market + "_" + "p7_t1_nationwide", dataList_p7_t1_nationwide, valueTypeList_p7_t1_nationwide,
			filterList_p7_t1_nationwide, limitNum_p7_t1_nationwide,
			sortOrderAsc, otherTag_noOther, selectList, titleList_p7_t1_nationwide, factory_nomal, keyProdListNomal)

		//全国，p7_t2_nationwide
		val dataList_p7_t2_nationwide = List("2018Q4YTD")
		val valueTypeList_p7_t2_nationwide = List("sales", "growth", "share", "shareGrowth", "EI")
		val filterList_p7_t2_nationwide: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List("全国")),
			("date", dataList_p7_t2_nationwide),
			("valueType", valueTypeList_p7_t2_nationwide)
		)
		val limitNum_p7_t2_nationwide = 13
		val selectList_p7_t2_nationwide = List("rank", "key", "mole_name", "corp_name")
		val titleList_p7_t2_nationwide = List("市场排名", "重点产品", "分子", "公司","销量(Mn)", "销量同比增长", "marketShare(%)", "marketShare同比增长", "EI")
		val p7_t2_nationwide = pptInputData(market + "_" + "p7_t2_nationwide", dataList_p7_t2_nationwide, valueTypeList_p7_t2_nationwide,
			filterList_p7_t2_nationwide, limitNum_p7_t2_nationwide, sortOrderDesc, otherTag_normal, selectList_p7_t2_nationwide,
			titleList_p7_t2_nationwide, factory_nomal, keyProdListNomal)

		//全国，p8_t1_nationwide
		val dataList_p8_t1_nationwide = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p8_t1_nationwide = List("sales")
		val filterList_p8_t1_nationwide: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List("全国")),
			("date", dataList_p8_t1_nationwide),
			("valueType", valueTypeList_p8_t1_nationwide)
		)
		val titleList_p8_t1_nationwide = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val limitNum_p8_t1_nationwide = 12
		val selectList_p8_t1_nationwide = List("key")
		val p8_t1_nationwide = pptInputData(market + "_" + "p8_t1_nationwide", dataList_p8_t1_nationwide, valueTypeList_p8_t1_nationwide,
			filterList_p8_t1_nationwide, limitNum_p8_t1_nationwide, sortOrderAsc, otherTag_normal,
			selectList_p8_t1_nationwide, titleList_p8_t1_nationwide, factory_nomal, keyProdListNomal)

		//全国，p9_t1_nationwide
		val dataList_p9_t1_nationwide = List("2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val valueTypeList_p9_t1_nationwide = List("sales", "share")
		val filterList_p9_t1_nationwide: List[(String, List[String])] = List(
			("market", List(marketCN)),
			("keyType", List("prod")),
			("city", List("全国")),
			("date", dataList_p9_t1_nationwide),
			("valueType", valueTypeList_p9_t1_nationwide)
		)
		val limitNum_p9_t1_nationwide = 13
		val selectList_p9_t1_nationwide = List("key")
		val titleList_p9_t1_nationwide = List("", "2018Q1", "2018Q2", "2018Q3", "2018Q4")
		val p9_t1_nationwide = pptInputData(market + "_" + "p9_t1_nationwide", dataList_p9_t1_nationwide, valueTypeList_p9_t1_nationwide,
			filterList_p9_t1_nationwide, limitNum_p9_t1_nationwide, sortOrderAsc, otherTag_noOther,
			selectList_p9_t1_nationwide, titleList_p9_t1_nationwide, factory_nomal, keyProdListNomal)

		List(p4_t1_nationwide, p5_t1_nationwide, p6_t1_nationwide, p7_t1_nationwide, p7_t2_nationwide,
			p8_t1_nationwide, p9_t1_nationwide)
	}

	def getAllCityList(cityList: List[String]): List[pptInputData] ={
		cityList.map(x => getCityList(x)).reduce((l1, l2) => l1 ++ l2)
	}

	def getAllTableList(): List[pptInputData] ={
		getNationwideList() ++ getAllCityList(cityList)
	}
}

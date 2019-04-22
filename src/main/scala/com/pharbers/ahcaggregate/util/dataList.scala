package com.pharbers.ahcaggregate.util

object dataList extends Serializable {
	val dataListCommon = List("2017Q3YTD", "2018Q3YTD")
	val valueTypeList = List("sales", "share")
	val filterList: List[(String, List[String])] = List(
		("market", List("降糖药市场")),
		("keyType", List("oad")),
		("key", List("其他", "GLP-1激动剂", "DPP4抑制剂", "格列酮类", "格列奈类", "磺脲类", "双胍类", "糖苷类")),
		("city", List("北京")),
		("date", dataListCommon),
		("valueType", valueTypeList)
	)
	val mergeList = List("date", "valueType")
	val poivtList = List("key")
	val sortList = List("2018Q3YTD")
	val selectList = List("key") ++ valueTypeList.flatMap(x => dataListCommon.map(y => y + x))
	val tatleArray = Array(Array("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD", "2018Q3YTD"))
}

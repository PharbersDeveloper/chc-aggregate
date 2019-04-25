package com.pharbers.ahcaggregate.moudle

import org.apache.spark.sql.Column

case class pptInputData(
	                       tableIndex: String,
	                       dataList: List[String],
	                       valueTypeList: List[String],
	                       filterList: List[(String, List[String])],
	                       mergeList: List[String],
	                       poivtList: List[String],
	                       limitNum: Int,
	                       sortMap: Map[String, Column],
	                       selectList: List[String],
	                       titleList: List[List[String]]
                       ) {

}

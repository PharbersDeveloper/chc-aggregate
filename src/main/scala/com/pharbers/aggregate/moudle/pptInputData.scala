package com.pharbers.aggregate.moudle

import org.apache.spark.sql.{Column, DataFrame}

case class pptInputData(
	                       tableIndex: String,
	                       dataList: List[String],
	                       valueTypeList: List[String],
	                       filterList: List[(String, List[String])],
	                       mergeList: List[String],
	                       poivtList: List[String],
	                       limitNum: Int,
	                       sortMap: Map[String, Column],
	                       sortStr: String,
	                       selectList: List[String],
	                       titleList: List[List[String]],
	                       factory: String,
	                       keyProdList: List[String]
                       ) {

}

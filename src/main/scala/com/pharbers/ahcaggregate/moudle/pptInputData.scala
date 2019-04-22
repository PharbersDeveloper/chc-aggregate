package com.pharbers.ahcaggregate.moudle

case class pptInputData(
	                       tableIndex: String,
	                       dataList: List[String],
	                       valueTypeList: List[String],
	                       filterList: List[(String, List[String])],
	                       mergeList: List[String],
	                       poivtList: List[String],
	                       sortList: List[String],
	                       selectList: List[String],
	                       titleList: List[List[String]]
                       ) {

}

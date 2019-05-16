package com.pharbers.aggregate.moudle

case class pptInputData(
	                       tableIndex: String,
	                       dataList: List[String],
	                       valueTypeList: List[String],
	                       filterList: List[(String, List[String])],
	                       limitNum: Int,
	                       sortOrder: String,
	                       otherTag: String,
	                       selectList: List[String],
	                       titleList: List[String],
	                       factory: String,
	                       keyProdList: List[String]
                       ) {

}

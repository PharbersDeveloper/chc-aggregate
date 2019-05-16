package com.pharbers.aggregate.moudle

case class chcMongleData(
	                        _id: String,
	                        tableIndex: String,
	                        cells: List[Map[String, String]]
                        ) extends Serializable {

}

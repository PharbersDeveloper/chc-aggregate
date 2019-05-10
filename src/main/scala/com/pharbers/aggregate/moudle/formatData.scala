package com.pharbers.aggregate.moudle

case class formatData(
	                     MARKET: String,
	                     CITY: String,
	                     TIME: String,
	                     PRODUCT_ID: String,
	                     SALES: Double,
	                     UNITS: Double,
	                     DEV_PRODUCT_NAME: String,
	                     DEV_MOLE_NAME: String,
	                     DEV_PACKAGE_DES: String,
	                     DEV_PACKAGE_NUMBER: String,
	                     DEV_CORP_NAME: String,
	                     IMS_DELIVERY_WAY: String,
	                     DEV_DOSAGE_NAME: String,
	                     DEV_PACK_ID: String,
	                     ATC3: String,
	                     OAD_TYPE: String,
	                     var product_id_list: List[String] = List(),
	                     var sales_list: List[Double] = List(),
	                     var units_list: List[Double] = List(),
	                     var mole_name_list: List[String] = List(),
	                     var package_list: List[String] = List(),
	                     var pack_num_list: List[String] = List(),
	                     var corp_name_list: List[String] = List(),
	                     var delivery_way_list: List[String] = List(),
	                     var dosage_name: List[String] = List(),
	                     var pack_id_list: List[String] = List(),
	                     var atc3_list: List[String] = List(),
	                     var oad_type_list: List[String] = List()
                     ) extends Serializable {

}

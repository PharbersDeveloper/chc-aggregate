package com.pharbers.aggregate.ppt

import com.pharbers.aggregate.moudle.pptInputData
import org.apache.spark.sql.DataFrame
import com.pharbers.data.util._

case class generatePPTData() extends phCommand {
	override def exec(dataObj: pptInputData, df: DataFrame): Unit ={
		val tableIndex = dataObj.tableIndex
		val dataList = dataObj.dataList
		val valueTypeList = dataObj.valueTypeList
		val filterList: List[(String, List[String])] = dataObj.filterList
		val limitNum = dataObj.limitNum
		val sortStr = dataObj.sortOrder
		val selectList = dataObj.selectList ++ valueTypeList
//		++ valueTypeList.flatMap(x => dataList.map(y => y + x))
		val titleList = dataObj.titleList
		val keyProdList = dataObj.keyProdList
		val otherTag = dataObj.otherTag
		val resultDF = aggregateForTable().getTableResult(df, filterList, selectList, keyProdList,
			dataList, titleList, valueTypeList, limitNum, sortStr, otherTag, tableIndex)
		resultDF.save2Mongo("chc-ppt")
	}
}

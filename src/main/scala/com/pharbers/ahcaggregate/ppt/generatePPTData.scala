package com.pharbers.ahcaggregate.ppt

import com.mongodb.casbah.commons.MongoDBObject
import com.pharbers.ahcaggregate.moudle.pptInputData
import com.pharbers.ahcaggregate.util.save2Mongo
import org.apache.spark.sql.DataFrame

case class generatePPTData() {
	def generateData(dataObj: pptInputData, df: DataFrame): Unit ={
		val tableIndex = dataObj.tableIndex
		val dataList = dataObj.dataList
		val valueTypeList = dataObj.valueTypeList
		val filterList: List[(String, List[String])] = dataObj.filterList
		val mergeList = dataObj.mergeList
		val poivtList = dataObj.poivtList
		val sortList = dataObj.sortList
		val selectList = dataObj.selectList ++ valueTypeList.flatMap(x => dataList.map(y => y + x))
		val titleList = dataObj.titleList
		val resultArray = titleList ++ aggregateForTable().getTableResult(df, filterList, mergeList, poivtList, selectList,sortList)
		val resultMap = resultArray.zipWithIndex.flatMap { case (arr, idx1) =>
			arr.zipWithIndex.map{case (value, idx2) =>
				Map("coordinate" -> ((idx2 + 65).toChar + (idx1+1).toString), "value" -> value)
			}
		}.map(x => x.filter(y => y._2 != ""))
		save2Mongo().save(MongoDBObject("tableIndex" -> tableIndex, "cells" -> resultMap))
	}
}

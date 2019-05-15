package com.pharbers.aggregate.ppt

import com.mongodb.casbah.commons.MongoDBObject
import com.pharbers.aggregate.moudle.pptInputData
import com.pharbers.aggregate.util.save2Mongo
import org.apache.spark.sql.DataFrame

case class generateDataWithIndex() extends phCommand {
	override def exec(dataObj: pptInputData, df: DataFrame): Unit ={
		val tableIndex = dataObj.tableIndex
		val dataList = dataObj.dataList
		val valueTypeList = dataObj.valueTypeList
		val filterList: List[(String, List[String])] = dataObj.filterList
		val mergeList = dataObj.mergeList
		val poivtList = dataObj.poivtList
		val limitNum = dataObj.limitNum
		val sortMap = dataObj.sortMap
		val sortStr = dataObj.sortStr
		val selectList = dataObj.selectList ++ valueTypeList.flatMap(x => dataList.map(y => y + x))
		val titleList = dataObj.titleList
		val keyProdList = dataObj.keyProdList
		val dataResult = aggregateForTable().getTableResult(df, filterList, mergeList, poivtList, selectList,
			limitNum,sortMap, sortStr, keyProdList)
		val rankedResult = dataResult.zipWithIndex.map{case (lst, idx) => List((idx + 1).toString) ++ lst}
		val resultArray = titleList ++ rankedResult
		val resultMap = resultArray.zipWithIndex.flatMap { case (arr, idx1) =>
			arr.zipWithIndex.map{case (value, idx2) =>
				Map("coordinate" -> ((idx2 + 65).toChar + (idx1+1).toString), "value" -> value)
			}
		}
		save2Mongo().save(MongoDBObject("tableIndex" -> tableIndex, "cells" -> resultMap))
	}
}
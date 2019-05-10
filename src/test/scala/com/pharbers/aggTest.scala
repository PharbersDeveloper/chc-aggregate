//package com.pharbers
//
//import com.mongodb.casbah.commons.MongoDBObject
//import com.pharbers.ahcaggregate.common.phFactory
//import com.pharbers.ahcaggregate.ppt.{aggregateForPPT, aggregateForTable}
//import com.pharbers.ahcaggregate.util.{save2Mongo, saveDF2mongo}
//import com.pharbers.spark.util.readCsv
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.functions._
//
//object aggTest extends App {
//	val sprakDriver = phFactory.getSparkInstance()
//
//	import sprakDriver.conn_instance
//
//	import sprakDriver.ss.implicits._
//
//	val chcDF = sprakDriver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
//		.select("city", "Date", "OAD类别", "Molecule_Desc", "分子", "Prod_Desc", "MNF_Desc", "Sales")
//		.na.fill("")
//		.withColumn("Sales", col("Sales").cast(DoubleType))
//		.withColumn("market", lit("降糖药市场"))
//	val resultDF = aggregateForPPT().getAggregate(chcDF)
//
//	//table
//	val dataList = List("2017Q3YTD", "2018Q3YTD")
//	val valueTypeList = List("sales", "share")
//	val filterList: List[(String, List[String])] = List(
//		("market", List("降糖药市场")),
//		("keyType", List("oad")),
//		("key", List("其他", "GLP-1激动剂", "DPP4抑制剂", "格列酮类", "格列奈类", "磺脲类", "双胍类", "糖苷类")),
//		("city", List("北京")),
//		("date", dataList),
//		("valueType", valueTypeList)
//	)
//	val mergeList = List("date", "valueType")
//	val poivtList = List("key")
//	val sortList = List(col("2018Q3YTD"))
//	val selectList = List("key") ++ valueTypeList.flatMap(x => dataList.map(y => y + x))
//	val tatleArray = List(List("", "2017Q3YTD", "2018Q3YTD", "2017Q3YTD", "2018Q3YTD"))
//
//	val resultArray = tatleArray ++ aggregateForTable().getTableResult(resultDF, filterList, mergeList, poivtList, selectList,sortList)
//	val resultMap = resultArray.zipWithIndex.flatMap { case (arr, idx1) =>
//		arr.zipWithIndex.map{case (value, idx2) =>
//			Map("coordinate" -> ((idx2 + 65).toChar + (idx1+1).toString), "value" -> value)
//		}
//	}
//	val mongoResult = MongoDBObject("tableIndex" -> "p1-t1", "cells" -> resultMap)
//	save2Mongo().save(mongoResult)
//	resultArray.foreach(x =>{
//		x.foreach(y => print(y + "  |  "))
//		println()
//	}
//	)
//}

package com.pharbers.ahcaggregate.ppt

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

case class aggregateForTable() {
	//1. df filter
	val func_filter: (Row, List[String], String) => Boolean = (row, lst, str) => {
		lst.contains(row.getAs[String](str))
	}

	val func_mergecol: UserDefinedFunction = udf {
		(str1: String, str2: String) => str1 + str2
	}

	def df_filter(df: DataFrame, lst: List[(String, List[String])]): DataFrame = {
		if (lst.isEmpty) df
		else {
			df_filter(df.filter(row => func_filter(row, lst.head._2, lst.head._1)), lst.tail)
		}
	}

	//2. df col merge
	def df_merge(df: DataFrame, lst: List[String]): DataFrame = {
		df.withColumn("titles", func_mergecol(col(lst.head), col(lst.last)))
	}

	//3. df poivt
	def df_poivt(df: DataFrame, lst: List[String], str: String): DataFrame = {
		df.groupBy(lst.head, lst.tail: _*)
			.pivot(str)
			.sum("value")
			.na.fill(0)
	}

	//4. df select
	def df_select(df: DataFrame, lst: List[String]): DataFrame = {
		df.select(lst.head, lst.tail: _*)
	}


	//5. df sort
	def df_sort(df: DataFrame, lst: List[String]): DataFrame = {
		df.sort(lst.head, lst.tail: _*)
	}

	//6. df collect
	def df_collect(df: DataFrame): List[List[String]] = {
		val arr = df.collect().map(x => x.toSeq.toList).map(x => x.map(y => y.toString)).toList
		arr
	}

	def getTableResult(df: DataFrame, filterList: List[(String, List[String])], mergeList: List[String],
	                   poivtList: List[String], selectedList: List[String], sortList: List[String]): List[List[String]] = {
		val filteredDF = df_filter(df, filterList)
		val mergedDF = df_merge(filteredDF, mergeList)
		val poivtDF = df_poivt(mergedDF, poivtList, "titles")
		val selectedDF = df_select(poivtDF, selectedList)
		//		val shortedDF = df_sort(poivtDF, sortList)
		val result = df_collect(selectedDF)
		result
	}
}

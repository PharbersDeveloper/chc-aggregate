package com.pharbers.aggregate.ppt

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Row}
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

	val func_addCol: (DataFrame, List[String]) => DataFrame = (df, lst) => {
		if (lst.isEmpty) df
		else func_addCol(df.withColumn(lst.head, lit(0.0)), lst.tail)
	}

	//4. df select
	def df_select(df: DataFrame, lst: List[String]): DataFrame = {
		val colList = df.columns.toList
		val diffList = lst.diff(colList)
		val dfTemp = func_addCol(df, diffList)
		dfTemp.select(lst.head, lst.tail: _*)
	}


	//5. df sort
	def df_sort(df: DataFrame, limitNum: Int, sortMap: Map[String, Column], sortStr: String, keyProdList: List[String]): DataFrame = {
		val takeList = sortMap.values.toList.map(x => -x)
		val sortList = sortMap.map(x => {
			if (x._1 == "desc") -x._2
			else x._2
		}).toList
		val func_filter: UserDefinedFunction = udf {
			str: String => keyProdList.contains(str.split(31.toChar.toString).head)
		}
		val keyProductDF = df.filter(func_filter(col("key")))
		val dfTop = if (keyProdList.isEmpty) df.filter(col("key") =!= "其他").sort(takeList: _*).limit(limitNum)
			.union(keyProductDF)
			.sort(sortList: _*)
		else df.filter(col("key") =!= "其他" && !func_filter(col("key"))).sort(takeList: _*).limit(limitNum)
			.union(keyProductDF)
			.sort(sortList: _*)
		val func_nomal: DataFrame => DataFrame = dataframe => dataframe.filter(col("key") === "其他")
		val func_other: DataFrame => DataFrame = dataframe => {
			val func_filter: (String, List[String]) => Boolean = (str, lst) => {
				if (lst.contains(str)) false
				else true
			}
			val colList = dataframe.columns.filter(x => x != "key").map(x => sum(x).as(x))
			val keyList = dfTop.select("key").collect().map(x => x.getString(0)).toList
			val otherDF = dataframe.filter(x => func_filter(x.get(0).toString, keyList))
				.withColumn("key", lit("其他"))
				.groupBy("key")
				.agg(colList.head, colList.tail: _*)
			otherDF
		}
		val funcMap = Map("nomal" -> func_nomal, "other" -> func_other)
		val dfOther = funcMap(sortStr)(df)
		dfOther.union(dfTop)
	}

	//6. df collect
	def df_collect(df: DataFrame): List[List[String]] = {
		val arr = df.collect().map(x => x.toSeq.toList).map(x => x.map(y => y.toString)).toList
		arr
	}

	val func_key: UserDefinedFunction = udf {
		str: String => str.split(31.toChar.toString).head
	}

	def getTableResult(df: DataFrame, filterList: List[(String, List[String])], mergeList: List[String], poivtList: List[String],
	                   selectedList: List[String], limitNum: Int, sortMap: Map[String, Column], sortStr: String,
	                   keyProdList: List[String]): List[List[String]] = {
		val filteredDF = df_filter(df, filterList).filter(col("key") =!= "total")
		val mergedDF = df_merge(filteredDF, mergeList)
		val poivtDF = df_poivt(mergedDF, poivtList, "titles")
		val selectedDF = df_select(poivtDF, selectedList)
		val shortedDF = df_sort(selectedDF, limitNum, sortMap, sortStr, keyProdList)
			.withColumn("key", func_key(col("key")))
		val result = df_collect(shortedDF)
		result
	}
}

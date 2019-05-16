package com.pharbers.aggregate.ppt

import com.pharbers.aggregate.moudle.{afteraggredData, chcMongleData}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import com.pharbers.data.util._
import org.apache.spark.sql.types._
import org.bson.types.ObjectId

case class aggregateForTable() {
	//1. df filter
	val func_filter: (Row, List[String], String) => Boolean = (row, lst, str) => {
		lst.contains(row.getAs[String](str))
	}

	def df_filter(df: DataFrame, lst: List[(String, List[String])]): DataFrame = {
		if (lst.isEmpty) df
		else {
			df_filter(df.filter(row => func_filter(row, lst.head._2, lst.head._1)), lst.tail)
		}
	}

	def addColumnIndex(df: DataFrame) = sparkDriver.sqc.createDataFrame(
		// Add Column index
		df.rdd.zipWithIndex.map { case (row, columnindex) => Row.fromSeq(row.toSeq :+ (columnindex + 1)) },
		// Create schema
		StructType(df.schema.fields :+ StructField("rank", LongType, false))
	)


	//5. df sort
	def df_sort(df: DataFrame, keyProdList: List[String], limitNum: Int, sortOrder: String, otherTag: String,
	            rankData: String, rankValueType: String): DataFrame = {
		val sortMap = Map("asc" -> col("rank"), "desc" -> -col("rank"))
		val func_filter_keyProd: UserDefinedFunction = udf {
			str: String => keyProdList.contains(str.split(31.toChar.toString).head)
		}
		val rankDF = addColumnIndex(df.filter(col("valueType") === rankValueType &&
			col("date") === rankData && col("key") =!= "其他").sort(-col("value")))
		val keyProductDF = rankDF.filter(func_filter_keyProd(col("key")))

		val topDF = (if (keyProdList.isEmpty) rankDF.limit(limitNum)
		else rankDF.filter(!func_filter_keyProd(col("key"))).limit(limitNum).union(keyProductDF))
			.sort(sortMap(sortOrder))
		val keyList = topDF.select("key").collect().map(x => x.getString(0)).toList

		val func_filter_other: (String, List[String]) => Boolean = (str, lst) => {
			if (lst.contains(str)) true
			else false
		}
		val noOtherFunc: DataFrame => DataFrame = dataframe => {
			val formatList = List("key", "valueType", "date", "city", "keyType", "value")
			val colList = dataframe.columns.filter(x => !formatList.contains(x)).map(x => first(x).as(x))
			dataframe.filter(x => !func_filter_other(x.getAs[String]("key"), keyList))
				.withColumn("key", lit("其他"))
				.groupBy(formatList.head, formatList.tail.init: _*)
				.agg(sum("value").as("value"), colList: _*)
				.withColumn("rank", lit(0))
		}
		val haveOtherFunc: DataFrame => DataFrame = dataframe => dataframe.filter(col("key") === "其他")
			.withColumn("rank", lit(0))
		val otherDFMap = Map("noOther" -> noOtherFunc, "other" -> haveOtherFunc)
		import sparkDriver.ss.implicits._
		val topKeyDF = df.filter(x => func_filter_other(x.getAs[String]("key"), keyList))
			.join(topDF.select($"key".as("topKey"), $"rank"), col("key") === col("topKey"))
			.drop("topKey")
		val resultDF = if (otherTag == "normal") topKeyDF
		else otherDFMap(otherTag)(df).unionByName(topKeyDF)
		resultDF
	}

	//6. df collect
	def df_collect(df: DataFrame, dateList: List[String], selectList: List[String], titleList: List[String],
	               tableIndex: String, sortOrder: String): DataFrame = {
		import sparkDriver.ss.implicits._
		val valueListAll = List("sales", "share", "growth", "shareGrowth", "EI", "moleShare", "moleGrowth",
			"moleShareGrowth", "moleEI")
		val valueList = selectList.filter(x => valueListAll.contains(x))
		val lstSize = valueList.size * dateList.size
		val infoList = selectList.filter(x => !valueListAll.contains(x))
		val sortAsc: List[afteraggredData] => List[afteraggredData] = lst => lst.sortBy(x => -x.rank)
		val sortDesc: List[afteraggredData] => List[afteraggredData] = lst => lst.sortBy(x => x.rank)
		val sortMap = Map("asc" -> sortAsc, "desc" -> sortDesc)
		val func_sortRank: List[afteraggredData] => List[afteraggredData] = lst => {
			lst.filter(x => x.key == "其他") ++ sortMap(sortOrder)(lst.filter(x => x.key != "其他"))
		}
		val result = df.toJavaRDD.rdd.map(x => {
			val idx = valueList.indexOf(x.getAs[String]("valueType")) * dateList.size + dateList.indexOf(x.getAs[String]("date"))
			afteraggredData(x.getAs[String]("market"), x.getAs[String]("city"),
				x.getAs[String]("date"), x.getAs[String]("key"), x.getAs[String]("keyType"),
				x.getAs[Double]("value"), x.getAs[String]("valueType"),
				x.getAs[String]("product_name"), x.getAs[String]("mole_name"),
				x.getAs[String]("oad_type"), x.getAs[String]("package_des"),
				x.getAs[String]("pack_number"), x.getAs[String]("corp_name"),
				x.getAs[String]("delivery_way"), x.getAs[String]("dosage_name"),
				x.getAs[String]("product_id"), x.getAs[String]("pack_id"),
				x.getAs[String]("atc3"), rank = x.getAs[Long]("rank"),
				valueList = List.fill(idx)(0.0) ++ List(x.getAs[Double]("value")) ++ List.fill(lstSize - idx - 1)(0.0)
			)
		}).keyBy(x => x.key)
			.reduceByKey((left, rigth) => {
				left.valueList = left.valueList.zip(rigth.valueList).map(value => value._1 + value._2)
				left
			}).map(x => List(x._2))
			.keyBy(x => (tableIndex, tableIndex))
			.reduceByKey((left, rigth) => {
				left ++ rigth
			}).map(x => {
			val resultList = titleList :: func_sortRank(x._2).map(x => {
				val valueMap = x.getResultMap()
				infoList.map(infoKey => valueMap(infoKey)) ++ x.valueList.map(_.toString)
			})
			val resultMap = resultList.zipWithIndex.flatMap { case (arr, idx1) =>
				arr.zipWithIndex.map { case (value, idx2) =>
					Map("coordinate" -> ((idx2 + 65).toChar + (idx1 + 1).toString), "value" -> value)
				}
			}
			chcMongleData(ObjectId.get().toString, tableIndex, resultMap)
		}).toDF()
		result
	}

	val func_key: UserDefinedFunction = udf {
		str: String => str.split(31.toChar.toString).head
	}

	def getTableResult(df: DataFrame, filterList: List[(String, List[String])], selectedList: List[String],
	                   keyProdList: List[String], dateList: List[String], titleList: List[String],
	                   valueTypeList: List[String], limitNum: Int, sortOrder: String, otherTag: String,
	                   tableIndex: String): DataFrame = {
		val filteredDF = df_filter(df, filterList).filter(col("key") =!= "total")
		val shortedDF = df_sort(filteredDF, keyProdList, limitNum, sortOrder, otherTag, dateList.last, valueTypeList.head)
			.withColumn("key", func_key(col("key")))
		val result = df_collect(shortedDF, dateList, selectedList, titleList, tableIndex, sortOrder)
		result
	}
}

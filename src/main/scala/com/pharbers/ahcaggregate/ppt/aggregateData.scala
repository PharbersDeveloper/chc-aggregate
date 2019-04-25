package com.pharbers.ahcaggregate.ppt

import com.pharbers.ahcaggregate.moudle.afteraggredData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class aggregateData() extends Serializable {

	import com.pharbers.data.util._
	import sparkDriver.ss.implicits._

	val func_group: (DataFrame, List[String], List[Column], List[String], String, String) => DataFrame =
		(chcDF, groupList, aggfuncList, selectList, key, keyType) => {
			chcDF.groupBy(groupList.head, groupList.tail: _*)
				.agg(aggfuncList.head, aggfuncList.tail: _*)
				.withColumn("key", col(key))
				.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("sales"))
				.select(selectList.head, selectList.tail: _*)
		}

	def unionDF(dataframe: DataFrame, dfList: List[DataFrame]): DataFrame = {
		if (dfList.isEmpty) dataframe
		else unionDF(dataframe.union(dfList.head), dfList.tail)
	}

	def groupData(chcDF: DataFrame): DataFrame = {
		val selectList = List("MARKET", "CITY", "TIME", "key", "keyType", "SALES", "valueType", "PRODUCT_NAME",
			"MOLE_NAME", "OAD_TYPE", "PACKAGE_DES", "PACKAGE_NUMBER", "CORP_NAME", "DELIVERY_WAY", "DOSAGE_NAME",
			"PRODUCT_ID", "PACK_ID", "ATC3")

		val oad_groupList = List("MARKET", "CITY", "TIME", "OAD_TYPE")
		val prod_groupList = List("MARKET", "CITY", "TIME", "PRODUCT_NAME")
		val mole_groupList = List("MARKET", "CITY", "TIME", "MOLE_NAME")
		val corp_groupList = List("MARKET", "CITY", "TIME", "CORP_NAME")

		val oad_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("PRODUCT_NAME").as("PRODUCT_NAME"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("CORP_NAME").as("CORP_NAME"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"))
		val prod_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("OAD_TYPE").as("OAD_TYPE"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("CORP_NAME").as("CORP_NAME"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"))
		val mole_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("PRODUCT_NAME").as("PRODUCT_NAME"), first("OAD_TYPE").as("OAD_TYPE"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("CORP_NAME").as("CORP_NAME"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"))
		val corp_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("PRODUCT_NAME").as("PRODUCT_NAME"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("OAD_TYPE").as("OAD_TYPE"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"))
		val oadDF = func_group(chcDF, oad_groupList, oad_aggfuncList, selectList, "OAD_TYPE", "oad")
		val prodDF = func_group(chcDF, prod_groupList, prod_aggfuncList, selectList, "PRODUCT_NAME", "prod")
		val moleDF = func_group(chcDF, mole_groupList, mole_aggfuncList, selectList, "MOLE_NAME", "mole")
		val corpDF = func_group(chcDF, corp_groupList, corp_aggfuncList, selectList, "CORP_NAME", "corp")
		oadDF.union(prodDF).union(moleDF).union(corpDF)
	}

	def YTDSales(groupedDF: DataFrame): DataFrame = {
		val func_getYTDData: String => List[String] = data => {
			val yqList = data.split("Q")
			val quaterList = Range(1, yqList.last.toInt + 1).toList
			quaterList.map(x => yqList.head + "Q" + x.toString).distinct
		}
		groupedDF.toJavaRDD.rdd.map(x => afteraggredData(x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString,
			x.get(4).toString, x.get(5).toString.toDouble, x.get(6).toString, x.get(7).toString, x.get(8).toString, x.get(9).toString,
			x.get(10).toString, x.get(11).toString, x.get(12).toString, x.get(13).toString, x.get(14).toString, x.get(15).toString,
			x.get(16).toString, x.get(17).toString, salesList = List(x.get(5).toString.toDouble), dateList = List(x.get(2).toString)))
			.keyBy(x => (x.market, x.city, x.keyType, x.key))
			.reduceByKey((left, right) => {
				left.dateList = left.dateList ++ right.dateList
				left.salesList = left.salesList ++ right.salesList
				left
			}).flatMap(x => {
			val dataList = x._2.dateList.distinct
			val YTDData = dataList.map(x => func_getYTDData(x))
			val YTDDataMap = dataList.zip(YTDData).filter(y => y._2.length == y._2.intersect(dataList).length && y._2.nonEmpty)
			val resultMap = YTDDataMap.map(y => {
				y._1 -> y._2.map(date => x._2.salesList(x._2.dateList.indexOf(date))).sum
			}).toMap
			resultMap.map(y =>
				afteraggredData(x._2.market, x._2.city, y._1, x._2.key, x._2.keyType, y._2, x._2.valueType,
					x._2.product_name, x._2.mole_name, x._2.oad_type, x._2.package_des, x._2.pack_number,
					x._2.corp_name, x._2.delivery_way, x._2.dosage_name, x._2.product_id, x._2.pack_id, x._2.atc3))
		}).toDF()
	}

	//TODO: 优化keyBy的List，这种写法可以，但是太恶心了
	def getShareDF(groupedDF: DataFrame): RDD[afteraggredData] = {
		groupedDF.toJavaRDD.rdd.map(x => afteraggredData(x.get(0).toString, x.get(1).toString, x.get(2).toString,
			x.get(3).toString, x.get(4).toString, x.get(5).toString.toDouble, x.get(6).toString, x.get(7).toString,
			x.get(8).toString, x.get(9).toString, x.get(10).toString, x.get(11).toString, x.get(12).toString,
			x.get(13).toString, x.get(14).toString, x.get(15).toString, x.get(16).toString, x.get(17).toString,
			salesList = List(x.get(5).toString.toDouble), keyList = List(x.get(3).toString),
			mole_nameList = List(x.get(8).toString), oad_typeList = List(x.get(9).toString),
			package_desList = List(x.get(10).toString), pack_numberList = List(x.get(11).toString),
			corp_nameList = List(x.get(12).toString), delivery_wayList = List(x.get(13).toString),
			dosage_nameList = List(x.get(14).toString), product_idList = List(x.get(15).toString),
			pack_idList = List(x.get(16).toString), atc3List = List(x.get(17).toString)))
			.keyBy(x => (x.market, x.date, x.city, x.keyType))
			.reduceByKey((left, right) => {
				left.salesList = left.salesList ++ right.salesList
				left.keyList = left.keyList ++ right.keyList
				left.mole_nameList = left.mole_nameList ++ right.mole_nameList
				left.oad_typeList = left.oad_typeList ++ right.oad_typeList
				left.package_desList = left.package_desList ++ right.package_desList
				left.pack_numberList = left.pack_numberList ++ right.pack_numberList
				left.corp_nameList = left.corp_nameList ++ right.corp_nameList
				left.delivery_wayList = left.delivery_wayList ++ right.delivery_wayList
				left.dosage_nameList = left.dosage_nameList ++ right.dosage_nameList
				left.product_idList = left.product_idList ++ right.product_idList
				left.pack_idList = left.pack_idList ++ right.pack_idList
				left.atc3List = left.atc3List ++ right.atc3List
				left
			}).flatMap(x => {
			val keyList = x._2.keyList ++ List("total")
			val salesListTemp = x._2.salesList
			val totalSales = salesListTemp.sum
			val salesList = x._2.salesList ++ List(totalSales)
			val shareList = salesList.map(y => (y / totalSales) * 100)
			val mole_nameList = x._2.mole_nameList ++ List("total")
			val oad_typeList = x._2.oad_typeList ++ List("total")
			val package_desList = x._2.package_desList ++ List("total")
			val pack_numberList = x._2.pack_numberList ++ List("total")
			val corp_nameList = x._2.corp_nameList ++ List("total")
			val delivery_wayList = x._2.delivery_wayList ++ List("total")
			val dosage_nameList = x._2.dosage_nameList ++ List("total")
			val product_idList = x._2.product_idList ++ List("total")
			val pack_idList = x._2.pack_idList ++ List("total")
			val atc3List = x._2.atc3List ++ List("total")
			val salesData = keyList.map(y => {
				val idx = keyList.indexOf(y)
				afteraggredData(x._2.market, x._2.city, x._2.date, y, x._2.keyType, salesList(idx), "sales",
					x._2.product_name, mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx), pack_idList(idx),
					atc3List(idx))
			})
			val shareData = keyList.map(y => {
				val idx = keyList.indexOf(y)
				afteraggredData(x._2.market, x._2.city, x._2.date, y, x._2.keyType, shareList(idx), "share",
					x._2.product_name, mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx), pack_idList(idx),
					atc3List(idx))
			})
			salesData ++ shareData
		})
	}

	def growth(rdd: RDD[afteraggredData], func_value: afteraggredData => afteraggredData,
	           func_growth: (Map[String, String], afteraggredData) => Seq[afteraggredData]): RDD[afteraggredData] = {
		val func_growthDate: List[String] => Map[String, String] = lst => {
			lst.distinct.map(x => {
				val yqList = x.split("Q")
				val year = yqList.head
				((year.toInt + 1).toString + "Q" + yqList.last) -> x
			}).toMap.filter(x => lst.contains(x._1))
		}
		rdd.filter(x => x.value != 0)
			.map(x => {
				x.dateList = List(x.date)
				func_value(x)
			})
			.keyBy(x => (x.market, x.city, x.keyType, x.key, x.valueType))
			.reduceByKey((left, right) => {
				left.dateList = left.dateList ++ right.dateList
				left.salesList = left.salesList ++ right.salesList
				left.shareList = left.shareList ++ right.shareList
				left
			}).map(x => {
			val growthQuaterMap = func_growthDate(x._2.dateList)
			(growthQuaterMap, x._2)
		}).filter(x => x._1.nonEmpty)
			.flatMap(x => {
				val growthQuaterMap = x._1
				func_growth(growthQuaterMap, x._2)
			})
	}

	def salesGrowth(rdd: RDD[afteraggredData]): RDD[afteraggredData] = {
		val func_value: afteraggredData => afteraggredData = data => {
			data.salesList = List(data.value)
			data
		}
		val func_growth: (Map[String, String], afteraggredData) => Seq[afteraggredData] = (growthQuaterMap, data) => {
			val growthDateSeq = growthQuaterMap.keys.toSeq
			val growthValue = growthQuaterMap.map(dateMap => {
				val quaterValue = data.salesList(data.dateList.indexOf(dateMap._1))
				val lastQuaterValue = data.salesList(data.dateList.indexOf(dateMap._2))
				((quaterValue - lastQuaterValue) / lastQuaterValue) * 100
			}).toList
			growthDateSeq.map(date => afteraggredData(data.market, data.city, date, data.key, data.keyType, growthValue(growthDateSeq.indexOf(date)),
				"growth", data.product_name, data.mole_name, data.oad_type, data.package_des, data.pack_number,
				data.corp_name, data.delivery_way, data.dosage_name, data.product_id, data.pack_id, data.atc3))
		}
		growth(rdd, func_value, func_growth)
	}

	def shareGrowth(rdd: RDD[afteraggredData]): RDD[afteraggredData] = {
		val func_value: afteraggredData => afteraggredData = data => {
			data.shareList = List(data.value)
			data
		}
		val func_growth: (Map[String, String], afteraggredData) => Seq[afteraggredData] = (growthQuaterMap, data) => {
			val growthDateSeq = growthQuaterMap.keys.toSeq
			val growthValue = growthQuaterMap.map(dateMap => {
				val quaterValue = data.shareList(data.dateList.indexOf(dateMap._1))
				val lastQuaterValue = data.shareList(data.dateList.indexOf(dateMap._2))
				quaterValue - lastQuaterValue
			}).toList
			growthDateSeq.map(date => afteraggredData(data.market, data.city, date, data.key, data.keyType, growthValue(growthDateSeq.indexOf(date)),
				"shareGrowth", data.product_name, data.mole_name, data.oad_type, data.package_des, data.pack_number,
				data.corp_name, data.delivery_way, data.dosage_name, data.product_id, data.pack_id, data.atc3))
		}
		growth(rdd, func_value, func_growth)
	}

	def aggregateEI(rdd: RDD[afteraggredData]): RDD[afteraggredData] = {
		rdd.map(x => {
			x.keyList = List(x.key)
			x.growthList = List(x.value)
			x.mole_nameList = List(x.mole_name)
			x.oad_typeList = List(x.oad_type)
			x.package_desList = List(x.package_des)
			x.pack_numberList = List(x.pack_number)
			x.corp_nameList = List(x.corp_name)
			x.delivery_wayList = List(x.delivery_way)
			x.dosage_nameList = List(x.dosage_name)
			x.product_idList = List(x.product_id)
			x.pack_idList = List(x.pack_id)
			x.atc3List = List(x.atc3)
			x
		}).keyBy(x => (x.market, x.city, x.date, x.keyType))
			.reduceByKey((left, right) => {
				left.keyList = left.keyList ++ right.keyList
				left.growthList = left.growthList ++ right.growthList
				left.mole_nameList = left.mole_nameList ++ right.mole_nameList
				left.oad_typeList = left.oad_typeList ++ right.oad_typeList
				left.package_desList = left.package_desList ++ right.package_desList
				left.pack_numberList = left.pack_numberList ++ right.pack_numberList
				left.corp_nameList = left.corp_nameList ++ right.corp_nameList
				left.delivery_wayList = left.delivery_wayList ++ right.delivery_wayList
				left.dosage_nameList = left.dosage_nameList ++ right.dosage_nameList
				left.product_idList = left.product_idList ++ right.product_idList
				left.pack_idList = left.pack_idList ++ right.pack_idList
				left.atc3List = left.atc3List ++ right.atc3List
				left
			}).flatMap(x => {
			val marketGrowth = x._2.growthList(x._2.keyList.indexOf("total"))
			val mole_nameList = x._2.mole_nameList ++ List("total")
			val oad_typeList = x._2.oad_typeList ++ List("total")
			val package_desList = x._2.package_desList ++ List("total")
			val pack_numberList = x._2.pack_numberList ++ List("total")
			val corp_nameList = x._2.corp_nameList ++ List("total")
			val delivery_wayList = x._2.delivery_wayList ++ List("total")
			val dosage_nameList = x._2.dosage_nameList ++ List("total")
			val product_idList = x._2.product_idList ++ List("total")
			val pack_idList = x._2.pack_idList ++ List("total")
			val atc3List = x._2.atc3List ++ List("total")
			x._2.keyList.map(key => {
				val idx = x._2.keyList.indexOf(key)
				val value = ((x._2.growthList(idx) + 1) / (marketGrowth + 1)) * 100
				afteraggredData(x._2.market, x._2.city, x._2.date, key, x._2.keyType, value, "EI", x._2.product_name,
					mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx),
					pack_idList(idx), atc3List(idx))
			})
		})

	}

	def getAggregate(chcDF: DataFrame): DataFrame = {
		val func_YTDDate: UserDefinedFunction = udf {
			date: String => date + "YTD"
		}
		val getResult: DataFrame => DataFrame = df => {
			val shareResult = getShareDF(df)
			val salseRDD = shareResult.filter(x => x.valueType == "sales")
			val shareRDD = shareResult.filter(x => x.valueType == "share")
			val salesGrowthRDD = salesGrowth(salseRDD)
			val shareGrowthRDD = shareGrowth(shareRDD)
			val EIRDD = aggregateEI(salesGrowthRDD)
			val dfList = List(salseRDD, shareRDD, salesGrowthRDD, shareGrowthRDD, EIRDD).map(x => x.toDF())
			unionDF(dfList.head, dfList.tail)
		}
		val resultList = List("market", "city", "date", "key", "keyType", "value", "valueType", "product_name", "mole_name",
			"oad_type", "package_des", "pack_number", "corp_name", "delivery_way", "dosage_name", "product_id", "pack_id",
			"atc3")
		val groupedDF = groupData(chcDF)
		val YTDGrowpedDF = YTDSales(groupedDF)
		val quaterResult = getResult(groupedDF).select(resultList.head, resultList.tail: _*)
		val YTDResult = getResult(YTDGrowpedDF).withColumn("date", func_YTDDate(col("date")))
			.select(resultList.head, resultList.tail: _*)
		val resultDF = quaterResult.union(YTDResult)
		resultDF
	}
}

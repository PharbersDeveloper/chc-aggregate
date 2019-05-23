package com.pharbers.aggregate.ppt

import com.pharbers.aggregate.moudle.{afteraggredData, formatData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

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
		val chcDFTemp = chcDF.withColumnRenamed("DEV_PRODUCT_NAME", "PRODUCT_NAME")
			.withColumnRenamed("DEV_MOLE_NAME", "MOLE_NAME")
			.withColumnRenamed("DEV_PACKAGE_DES", "PACKAGE_DES")
			.withColumnRenamed("DEV_PACKAGE_NUMBER", "PACKAGE_NUMBER")
			.withColumnRenamed("DEV_CORP_NAME", "CORP_NAME")
			.withColumnRenamed("IMS_DELIVERY_WAY", "DELIVERY_WAY")
			.withColumnRenamed("DEV_DOSAGE_NAME", "DOSAGE_NAME")
			.withColumnRenamed("DEV_PACK_ID", "PACK_ID")

		val oad_groupList = List("MARKET", "CITY", "TIME", "OAD_TYPE")
		//		val prod_groupList = List("MARKET", "CITY", "TIME", "PRODUCT_NAME", "MOLE_NAME", "PACKAGE_DES", "PACKAGE_NUMBER", "CORP_NAME", "PACK_ID")
		val prod_groupList = List("MARKET", "CITY", "TIME", "PRODUCT_NAME", "CORP_NAME")
		val mole_groupList = List("MARKET", "CITY", "TIME", "MOLE_NAME")
		val corp_groupList = List("MARKET", "CITY", "TIME", "CORP_NAME")
		val city_groupList = List("MARKET", "CITY", "TIME")

		val oad_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("PRODUCT_NAME").as("PRODUCT_NAME"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("CORP_NAME").as("CORP_NAME"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"))
		val prod_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("OAD_TYPE").as("OAD_TYPE"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("ATC3").as("ATC3"), first("PACK_ID").as("PACK_ID"))
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
		val city_aggfuncList = List(first("PRODUCT_ID").as("PRODUCT_ID"),
			sum("SALES").as("SALES"), sum("UNITS").as("UNITS"),
			first("PRODUCT_NAME").as("PRODUCT_NAME"), first("MOLE_NAME").as("MOLE_NAME"),
			first("PACKAGE_DES").as("PACKAGE_DES"), first("PACKAGE_NUMBER").as("PACKAGE_NUMBER"),
			first("OAD_TYPE").as("OAD_TYPE"), first("DELIVERY_WAY").as("DELIVERY_WAY"),
			first("DOSAGE_NAME").as("DOSAGE_NAME"), first("PACK_ID").as("PACK_ID"),
			first("ATC3").as("ATC3"), first("CORP_NAME").as("CORP_NAME"))
		val func_mnf: UserDefinedFunction = udf {
			(str1: String, str2: String) => str1 + 31.toChar.toString + str2
		}
		val oadDF = func_group(chcDFTemp, oad_groupList, oad_aggfuncList, selectList, "OAD_TYPE", "oad")
		val prodDF = func_group(chcDFTemp, prod_groupList, prod_aggfuncList, selectList, "PRODUCT_NAME", "prod")
			.withColumn("key", func_mnf(col("key"), col("CORP_NAME")))
		val moleDF = func_group(chcDFTemp, mole_groupList, mole_aggfuncList, selectList, "MOLE_NAME", "mole")
		val corpDF = func_group(chcDFTemp, corp_groupList, corp_aggfuncList, selectList, "CORP_NAME", "corp")
		val cityDF = func_group(chcDFTemp, city_groupList, city_aggfuncList, selectList, "CITY", "city")
		oadDF.union(prodDF).union(moleDF).union(corpDF).union(cityDF)
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
			val dateList = x._2.dateList
			val salesList = x._2.salesList
			val dateSalesList = dateList.zip(salesList)
			val date_ytd = dateList.map(x => x.split("Q").head).distinct.flatMap(x => Range(1, 5).map(y => x + "Q" + y.toString))
			val YTDData = date_ytd.map(x => x -> func_getYTDData(x))
			val resultMap = YTDData.map(y => {
				y._1 -> y._2.map(date => {
					val idx = dateList.indexOf(date)
					if (idx != -1) dateSalesList.filter(x => x._1 == date).map(x => x._2).sum
					else 0
				}).sum
			}).toMap
			resultMap.map(y =>
				afteraggredData(x._2.market, x._2.city, y._1, x._2.key, x._2.keyType, y._2, x._2.valueType,
					x._2.product_name, x._2.mole_name, x._2.oad_type, x._2.package_des, x._2.pack_number,
					x._2.corp_name, x._2.delivery_way, x._2.dosage_name, x._2.product_id, x._2.pack_id, x._2.atc3))
		}).toDF()
	}

	//TODO: 优化keyBy的List，这种写法可以，但是太恶心了
	def getShareDF(groupedDF: DataFrame, valueType: String): RDD[afteraggredData] = {
		val func_shareKey: afteraggredData => List[String] = data => List(data.market, data.date, data.city, data.keyType)
		val func_moleShareKey: afteraggredData => List[String] = data => List(data.market, data.date, data.city,
			data.keyType, data.mole_name)
		val func_nationwideShare: afteraggredData => List[String] = data => List(data.market, data.date, data.keyType)
		val func_nationwideMoleShare: afteraggredData => List[String] = data => List(data.market, data.date, data.keyType,
			data.mole_name)
		val func_cityKey: afteraggredData => List[String] = data => List(data.market, data.date, data.keyType)
		val keyMap = Map("share" -> func_shareKey, "moleShare" -> func_moleShareKey, "nationwideShare" -> func_nationwideShare,
			"cityShare" -> func_cityKey, "nationwideMoleShare" -> func_nationwideMoleShare)
		val func_shareFlat: afteraggredData => List[afteraggredData] = data => {
			val keyListTemp = data.keyList.distinct
			val keyList = keyListTemp ++ List("total")
			val keyListNormal = data.keyList ++ List("total")
			val kvListTemp = data.keyList.zip(data.salesList)
			val kvList = keyListTemp.map(key => key -> kvListTemp.filter(x => x._1 == key).map(x => x._2).sum)
			val totalSales = kvList.map(x => x._2).sum
			val salesList = kvList.map(x => x._2) ++ List(totalSales)
			val shareList = salesList.map(y => y / totalSales)
			val mole_nameList = data.mole_nameList ++ List(data.mole_nameList.last)
			val oad_typeList = data.oad_typeList ++ List(data.oad_typeList.last)
			val package_desList = data.package_desList ++ List("total")
			val pack_numberList = data.pack_numberList ++ List("total")
			val corp_nameList = data.corp_nameList ++ List("total")
			val delivery_wayList = data.delivery_wayList ++ List("total")
			val dosage_nameList = data.dosage_nameList ++ List("total")
			val product_idList = data.product_idList ++ List("total")
			val pack_idList = data.pack_idList ++ List("total")
			val atc3List = data.atc3List ++ List("total")
			val salesData = keyList.zipWithIndex.map { case (key, idx) =>
				val idxnormal = keyListNormal.indexOf(key)
				afteraggredData(data.market, data.city, data.date, key, data.keyType, salesList(idx), "sales",
					data.product_name, mole_nameList(idxnormal), oad_typeList(idxnormal), package_desList(idxnormal),
					pack_numberList(idxnormal), corp_nameList(idxnormal), delivery_wayList(idxnormal),
					dosage_nameList(idxnormal), product_idList(idxnormal), pack_idList(idxnormal), atc3List(idxnormal))
			}
			val shareData = keyList.zipWithIndex.map { case (key, idx) =>
				val idxnormal = keyListNormal.indexOf(key)
				afteraggredData(data.market, data.city, data.date, key, data.keyType, shareList(idx), valueType,
					data.product_name, mole_nameList(idxnormal), oad_typeList(idxnormal), package_desList(idxnormal),
					pack_numberList(idxnormal), corp_nameList(idxnormal), delivery_wayList(idxnormal),
					dosage_nameList(idxnormal), product_idList(idxnormal), pack_idList(idxnormal), atc3List(idxnormal))
			}
			salesData ++ shareData
		}
		//		val func_moleShareFlat: afteraggredData => List[afteraggredData] = data => {
		//			val keyList = data.keyList
		//			val salesList = data.salesList
		//			val totalSales = salesList.sum
		//			val shareList = salesList.map(y => y / totalSales)
		//			val mole_nameList = data.mole_nameList
		//			val oad_typeList = data.oad_typeList
		//			val package_desList = data.package_desList
		//			val pack_numberList = data.pack_numberList
		//			val corp_nameList = data.corp_nameList
		//			val delivery_wayList = data.delivery_wayList
		//			val dosage_nameList = data.dosage_nameList
		//			val product_idList = data.product_idList
		//			val pack_idList = data.pack_idList
		//			val atc3List = data.atc3List
		//			val shareData = keyList.zipWithIndex.map { case (key, idx) =>
		//				afteraggredData(data.market, data.city, data.date, key, data.keyType, shareList(idx), "moleShare",
		//					data.product_name, mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
		//					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx), pack_idList(idx),
		//					atc3List(idx))
		//			}
		//			shareData
		//		}
		val func_nationwideShareFlat: afteraggredData => List[afteraggredData] = data => {
			val keyListTemp = data.keyList.distinct
			val keyList = keyListTemp ++ List("total")
			val keyListNormal = data.keyList ++ List("total")
			val kvListTemp = data.keyList.zip(data.salesList)
			val kvList = keyListTemp.map(key => key -> kvListTemp.filter(x => x._1 == key).map(x => x._2).sum)
			val totalSales = kvList.map(x => x._2).sum
			val salesList = kvList.map(x => x._2) ++ List(totalSales)
			val shareList = salesList.map(y => y / totalSales)
			val mole_nameList = data.mole_nameList ++ List(data.mole_nameList.last)
			val oad_typeList = data.oad_typeList ++ List(data.oad_typeList.last)
			val package_desList = data.package_desList ++ List("total")
			val pack_numberList = data.pack_numberList ++ List("total")
			val corp_nameList = data.corp_nameList ++ List("total")
			val delivery_wayList = data.delivery_wayList ++ List("total")
			val dosage_nameList = data.dosage_nameList ++ List("total")
			val product_idList = data.product_idList ++ List("total")
			val pack_idList = data.pack_idList ++ List("total")
			val atc3List = data.atc3List ++ List("total")
			val valueTypeMap = Map("nationwideShare" -> "share", "nationwideMoleShare" -> "moleShare")
			val salesData = keyList.zipWithIndex.map { case (key, idx) =>
				val idxNormal = keyListNormal.indexOf(key)
				afteraggredData(data.market, "全国", data.date, key, data.keyType, salesList(idx), "sales",
					data.product_name, mole_nameList(idxNormal), oad_typeList(idxNormal), package_desList(idxNormal),
					pack_numberList(idxNormal), corp_nameList(idxNormal), delivery_wayList(idxNormal),
					dosage_nameList(idxNormal), product_idList(idxNormal), pack_idList(idxNormal), atc3List(idxNormal))
			}
			val shareData = keyList.zipWithIndex.map { case (key, idx) =>
				val idxNormal = keyListNormal.indexOf(key)
				afteraggredData(data.market, "全国", data.date, key, data.keyType, shareList(idx), valueTypeMap(valueType),
					data.product_name, mole_nameList(idxNormal), oad_typeList(idxNormal), package_desList(idxNormal),
					pack_numberList(idxNormal), corp_nameList(idxNormal), delivery_wayList(idxNormal),
					dosage_nameList(idxNormal), product_idList(idxNormal), pack_idList(idxNormal), atc3List(idxNormal))
			}
			salesData ++ shareData
		}
		val func_cityShareFlat: afteraggredData => List[afteraggredData] = data => {
			val keyList = data.keyList
			val salesList = data.salesList
			val totalSales = salesList.sum
			val shareList = salesList.map(y => y / totalSales)
			val mole_nameList = data.mole_nameList
			val oad_typeList = data.oad_typeList
			val package_desList = data.package_desList
			val pack_numberList = data.pack_numberList
			val corp_nameList = data.corp_nameList
			val delivery_wayList = data.delivery_wayList
			val dosage_nameList = data.dosage_nameList
			val product_idList = data.product_idList
			val pack_idList = data.pack_idList
			val atc3List = data.atc3List
			val cityList = data.cityList
			val shareData = keyList.zipWithIndex.map { case (key, idx) =>
				afteraggredData(data.market, key, data.date, key, data.keyType, shareList(idx), "share",
					data.product_name, mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx), pack_idList(idx),
					atc3List(idx))
			}
			shareData
		}
		val flatFuncMap = Map("share" -> func_shareFlat, "moleShare" -> func_shareFlat, "nationwideShare" -> func_nationwideShareFlat,
			"cityShare" -> func_cityShareFlat, "nationwideMoleShare" -> func_nationwideShareFlat)
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
			.keyBy(x => keyMap(valueType)(x))
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
				left.cityList = left.cityList ++ right.cityList
				left
			}).flatMap(x => flatFuncMap(valueType)(x._2))
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

	def salesGrowth(rdd: RDD[afteraggredData], valueType: String): RDD[afteraggredData] = {
		val func_value: afteraggredData => afteraggredData = data => {
			data.salesList = List(data.value)
			data
		}
		val func_growth: (Map[String, String], afteraggredData) => Seq[afteraggredData] = (growthQuaterMap, data) => {
			val growthDateSeq = growthQuaterMap.keys.toSeq
			val growthValue = growthQuaterMap.map(dateMap => {
				val quaterValue = data.salesList(data.dateList.indexOf(dateMap._1))
				val lastQuaterValue = data.salesList(data.dateList.indexOf(dateMap._2))
				(quaterValue - lastQuaterValue) / lastQuaterValue
			}).toList
			growthDateSeq.map(date => afteraggredData(data.market, data.city, date, data.key, data.keyType, growthValue(growthDateSeq.indexOf(date)),
				valueType, data.product_name, data.mole_name, data.oad_type, data.package_des, data.pack_number,
				data.corp_name, data.delivery_way, data.dosage_name, data.product_id, data.pack_id, data.atc3))
		}
		growth(rdd, func_value, func_growth)
	}

	def shareGrowth(rdd: RDD[afteraggredData], valueType: String): RDD[afteraggredData] = {
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
				valueType, data.product_name, data.mole_name, data.oad_type, data.package_des, data.pack_number,
				data.corp_name, data.delivery_way, data.dosage_name, data.product_id, data.pack_id, data.atc3))
		}
		growth(rdd, func_value, func_growth)
	}

	def aggregateEI(rdd: RDD[afteraggredData], valueType: String): RDD[afteraggredData] = {
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
			val mole_nameList = x._2.mole_nameList ++ List(x._2.mole_nameList.last)
			val oad_typeList = x._2.oad_typeList ++ List(x._2.oad_typeList.last)
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
				val value = (x._2.growthList(idx) + 1) / (marketGrowth + 1)
				afteraggredData(x._2.market, x._2.city, x._2.date, key, x._2.keyType, value, valueType, x._2.product_name,
					mole_nameList(idx), oad_typeList(idx), package_desList(idx), pack_numberList(idx),
					corp_nameList(idx), delivery_wayList(idx), dosage_nameList(idx), product_idList(idx),
					pack_idList(idx), atc3List(idx))
			})
		})
	}

	def formatDF(df: DataFrame): DataFrame = {
		//		中美上海施贵宝制药有限公司
		//		德国默克公司
		val func_replace: UserDefinedFunction = udf {
			corp: String => {
				if (corp == "中美上海施贵宝制药有限公司") "德国默克公司"
				else corp
			}
		}

		val func_replace_mole: UserDefinedFunction = udf {
			mole: String => {
				if (mole == "牡蛎碳酸钙") "碳酸钙"
				else if (mole == "碳酸钙,维生素D3" || mole == "碳酸钙D3") "碳酸钙维D3"
				else mole
			}
		}

		val resultDF = df.select("PRODUCT_ID", "SALES", "UNITS", "DEV_PRODUCT_NAME", "DEV_MOLE_NAME",
			"DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_CORP_NAME", "IMS_DELIVERY_WAY", "DEV_DOSAGE_NAME", "DEV_PACK_ID",
			"TIME", "name", "ATC3", "OAD_TYPE", "MARKET")
			.withColumnRenamed("name", "CITY")
			.withColumn("DEV_PACKAGE_NUMBER", col("DEV_PACKAGE_NUMBER").cast(StringType))
			.withColumn("DEV_PACK_ID", col("DEV_PACK_ID").cast(StringType))
			.withColumn("DEV_CORP_NAME", func_replace(col("DEV_CORP_NAME")))
    		.withColumn("DEV_MOLE_NAME", func_replace_mole(col("DEV_MOLE_NAME")))
			.na.fill("")
		resultDF
	}

	def moleEI(rdd: RDD[afteraggredData], valueType: String): RDD[afteraggredData] = {
		val func_value: afteraggredData => afteraggredData = data => {
			data.salesList = List(data.value)
			data
		}
		val func_growth: (Map[String, String], afteraggredData) => Seq[afteraggredData] = (growthQuaterMap, data) => {
			val growthDateSeq = growthQuaterMap.keys.toSeq
			val growthValue = growthQuaterMap.map(dateMap => {
				val quaterValue = data.salesList(data.dateList.indexOf(dateMap._1))
				val lastQuaterValue = data.salesList(data.dateList.indexOf(dateMap._2))
				quaterValue / lastQuaterValue
			}).toList
			growthDateSeq.map(date => afteraggredData(data.market, data.city, date, data.key, data.keyType, growthValue(growthDateSeq.indexOf(date)),
				valueType, data.product_name, data.mole_name, data.oad_type, data.package_des, data.pack_number,
				data.corp_name, data.delivery_way, data.dosage_name, data.product_id, data.pack_id, data.atc3))
		}
		growth(rdd, func_value, func_growth)
	}

	def getAggregate(chcDF: DataFrame): DataFrame = {
		val func_YTDDate: UserDefinedFunction = udf {
			date: String => date + "YTD"
		}
		val getResult: DataFrame => DataFrame = df => {
			val df_keyWithcity = df.filter(col("keyType") =!= "city")
			val df_nationwide = df.filter(col("keyType") === "city")
			val shareResult = getShareDF(df_keyWithcity, "share").union(getShareDF(df_keyWithcity, "nationwideShare"))
				.union(getShareDF(df_nationwide, "cityShare"))
			//TODO: 全国的moleShare
			val nationwideMoleResult = getShareDF(df_keyWithcity, "nationwideMoleShare")
			val nationwidemoleSalesRDD = nationwideMoleResult.filter(x => x.valueType == "sales")
//			val nationwideMoleSalesGrowthRDD = salesGrowth(nationwidemoleSalesRDD, "growth")
			val nationwideMoleShareRDD = nationwideMoleResult.filter(x => x.valueType == "moleShare")
			val nationwideMoleEIRDD = moleEI(nationwideMoleShareRDD, "moleEI")
			val nationwideMoleShareGrowthRDD = shareGrowth(nationwideMoleShareRDD, "moleShareGrowth")

			//moleShare city
			val moleResult = getShareDF(df_keyWithcity, "moleShare")
			val moleSalesRDD = moleResult.filter(x => x.valueType == "sales")
//			val molesalesGrowthRDD = salesGrowth(moleSalesRDD, "growth")
			val moleShareRDD = moleResult.filter(x => x.valueType == "moleShare")
			val moleEIRDD = moleEI(moleShareRDD, "moleEI")
			val moleShareGrowthRDD = shareGrowth(moleShareRDD, "moleShareGrowth")

			val salseRDD = shareResult.filter(x => x.valueType == "sales")
			val shareRDD = shareResult.filter(x => x.valueType == "share")

			val salesGrowthRDD = salesGrowth(salseRDD, "growth")
			val shareGrowthRDD = shareGrowth(shareRDD, "shareGrowth")

			val EIRDD = aggregateEI(salesGrowthRDD, "EI")
			val dfList = List(salseRDD, shareRDD, salesGrowthRDD, shareGrowthRDD, EIRDD, moleShareRDD, moleShareGrowthRDD,
				moleEIRDD, nationwideMoleEIRDD, nationwideMoleShareGrowthRDD, nationwideMoleShareRDD).map(x => x.toDF())
			unionDF(dfList.head, dfList.tail)
		}
		val resultList = List("market", "city", "date", "key", "keyType", "value", "valueType", "product_name", "mole_name",
			"oad_type", "package_des", "pack_number", "corp_name", "delivery_way", "dosage_name", "product_id", "pack_id",
			"atc3")
		val formatedDF = formatDF(chcDF)
		val groupedDF = groupData(formatedDF)
		val YTDGrowpedDF = YTDSales(groupedDF)
		val quaterResult = getResult(groupedDF).select(resultList.head, resultList.tail: _*)
		val YTDResult = getResult(YTDGrowpedDF).withColumn("date", func_YTDDate(col("date")))
			.select(resultList.head, resultList.tail: _*)
		val resultDF = quaterResult.union(YTDResult)
		resultDF
	}
}

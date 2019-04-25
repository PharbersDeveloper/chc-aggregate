package com.pharbers.ahcaggregate.ppt

import com.pharbers.ahcaggregate.common.phFactory
import com.pharbers.ahcaggregate.moudle.aggredData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class aggregateForPPT() {
	private lazy val sprakDriver = phFactory.getSparkInstance()

	import sprakDriver.ss.implicits._

	val func_group: (DataFrame, List[String], String) => DataFrame = (df, groupKeyList, sumCol) => {
		df.groupBy(groupKeyList.head, groupKeyList.tail: _*)
			.sum(sumCol)
			.withColumnRenamed("sum(" + sumCol + ")", sumCol)
	}

	val func_enlarge: DataFrame => DataFrame = df => {
		df.flatMap(x => {
			val keySeq = x.getAs[Seq[String]](3)
			val valueSeq = x.getAs[Seq[Double]](4)
			keySeq.map(key => aggredData(x.getString(0), x.getString(1), x.getString(2), key, valueSeq(keySeq.indexOf(key))))
		}).toDF()
	}

	def unionDF(dataframe: DataFrame, dfList: List[DataFrame]): DataFrame = {
		if (dfList.isEmpty) dataframe
		else unionDF(dataframe.union(dfList.head), dfList.tail)
	}

	def aggregateSales(chcDF: DataFrame): Map[String, DataFrame] = {
		val oadDF = func_group(chcDF, List("market", "city", "Date", "OAD类别"), "Sales")
		val moleDF = func_group(chcDF, List("market", "city", "Date", "分子"), "Sales")
		val prodDF = func_group(chcDF, List("market", "city", "Date", "Prod_Desc"), "Sales")
		val mnfDF = func_group(chcDF, List("market", "city", "Date", "MNF_Desc"), "Sales")

		val sumOadDF = func_group(oadDF, List("market", "city", "Date"), "Sales")
			.withColumn("OAD类别", lit("total"))
			.withColumnRenamed("Date", "date")
			.withColumnRenamed("OAD类别", "key")
			.withColumnRenamed("Sales", "value")
			.select("market", "city", "date", "key", "value")
			.union(oadDF)
		val sumMoleDF = func_group(moleDF, List("market", "city", "Date"), "Sales")
			.withColumn("分子", lit("total"))
			.withColumnRenamed("Date", "date")
			.withColumnRenamed("分子", "key")
			.withColumnRenamed("Sales", "value")
			.select("market", "city", "date", "key", "value")
			.union(moleDF)
		val sumProdDF = func_group(prodDF, List("market", "city", "Date"), "Sales")
			.withColumn("Prod_Desc", lit("total"))
			.withColumnRenamed("Date", "date")
			.withColumnRenamed("Prod_Desc", "key")
			.withColumnRenamed("Sales", "value")
			.select("market", "city", "date", "key", "value")
			.union(prodDF)
		val sumMnfDF = func_group(mnfDF, List("market", "city", "Date"), "Sales")
			.withColumn("MNF_Desc", lit("total"))
			.withColumnRenamed("Date", "date")
			.withColumnRenamed("MNF_Desc", "key")
			.withColumnRenamed("Sales", "value")
			.select("market", "city", "date", "key", "value")
			.union(mnfDF)

		Map("oadSales" -> sumOadDF, "moleSales" -> sumMoleDF, "prodSales" -> sumProdDF, "mnfSales" -> sumMnfDF)
	}

	def aggregateYTDSales(salesData: DataFrame): DataFrame = {
		val func_getYTDData: String => List[String] = data => {
			val yqList = data.split("Q")
			val quaterList = Range(1, yqList.last.toInt + 1).toList
			quaterList.map(x => yqList.head + "Q" + x.toString).distinct
		}
		val YTDSalesDFTemp = salesData.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, Seq(x(2).toString), x(3).toString, Seq(x(4).toString.toDouble)))
			.keyBy(x => (x._1, x._2, x._4))
			.reduceByKey((left, right) => {
				(left._1, left._2, left._3 ++ right._3, left._4, left._5 ++ right._5)
			}).map(x => {
			val dataSeq = x._2._3.distinct
			val YTDData = dataSeq.map(date => func_getYTDData(date))
			val YTDDataMap = dataSeq.zip(YTDData).filter(y => y._2.length == y._2.intersect(dataSeq).length && y._2.nonEmpty)
			val resultMap = YTDDataMap.map(y => {
				y._1 -> y._2.map(date => x._2._5(x._2._3.indexOf(date))).sum
			}).toMap
			(x._2._1, x._2._2, resultMap.keys.toSeq, x._2._4, resultMap.values.toSeq)
		}).toDF("market", "city", "date", "key", "value")
			.select("market", "city", "key", "date", "value")
		func_enlarge(YTDSalesDFTemp)
			.withColumnRenamed("key", "DateTemp")
			.withColumnRenamed("Date", "key")
			.withColumnRenamed("DateTemp", "Date")
			.select("market", "city", "date", "key", "value")
	}

	def aggregateShare(salesData: DataFrame): DataFrame = {
		val func_groupShare: DataFrame => DataFrame = df => {
			df.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, x(2).toString, Seq(x(3).toString), Seq(x(4).toString.toDouble)))
				.keyBy(x => (x._1, x._2, x._3))
				.reduceByKey((left, right) => {
					(left._1, left._2, left._3, left._4 ++ right._4, left._5 ++ right._5)
				}).map(x => {
				val totalResult = x._2._5.max
				(x._1._1, x._1._2, x._2._3, x._2._4, x._2._5.map(sales => sales / totalResult))
			}).toDF("market", "city", "date", "key", "value")
		}
		func_enlarge(func_groupShare(salesData))
	}


	val func_growth: (DataFrame, (Double, Double) => Double) => DataFrame = (data, valueFunc) => {
		val func_growthDate: List[String] => Map[String, String] = lst => {
			lst.distinct.map(x => {
				val yqList = x.split("Q")
				val year = yqList.head
				((year.toInt + 1).toString + "Q" + yqList.last) -> x
			}).toMap.filter(x => lst.contains(x._1))
		}
		val growthDFTemp = data.filter(col("value") =!= 0)
			.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, Seq(x(2).toString), x(3).toString, Seq(x(4).toString.toDouble)))
			.keyBy(x => (x._1, x._2, x._4))
			.reduceByKey((left, right) => {
				(left._1, left._2, left._3 ++ right._3, left._4, left._5 ++ right._5)
			}).map(x => {
			val growthQuaterMap = func_growthDate(x._2._3.toList)
			(growthQuaterMap, x._2)
		}).filter(x => x._1.nonEmpty)
			.map(x => {
				val growthQuaterMap = x._1
				val growthDateSeq = growthQuaterMap.keys.toSeq
				val growthValue = growthQuaterMap.map(dateMap => {
					val quaterValue = x._2._5(x._2._3.indexOf(dateMap._1))
					val lastQuaterValue = x._2._5(x._2._3.indexOf(dateMap._2))
					valueFunc(quaterValue, lastQuaterValue)
				}).toSeq
				(x._2._1, x._2._2, growthDateSeq, x._2._4, growthValue)
			}).toDF("market", "city", "date", "key", "value")
			.select("market", "city", "key", "date", "value")
		func_enlarge(growthDFTemp)
			.withColumnRenamed("key", "DateTemp")
			.withColumnRenamed("date", "key")
			.withColumnRenamed("DateTemp", "date")
			.select("market", "city", "date", "key", "value")
	}

	def aggreagteShareGrowth(shareData: DataFrame): DataFrame = {
		val func_shareGrowth: (Double, Double) => Double = (quaterValue, lastQuaterValue) => {
			quaterValue - lastQuaterValue
		}
		func_growth(shareData, func_shareGrowth)
	}

	def aggregateGrowth(salesData: DataFrame): DataFrame = {
		val func_salesGrowth: (Double, Double) => Double = (quaterValue, lastQuaterValue) => {
			(quaterValue - lastQuaterValue) / lastQuaterValue
		}
		func_growth(salesData, func_salesGrowth)
	}

	def aggregateEI(grouthData: DataFrame): DataFrame = {
		val aggregateEITemp = grouthData.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, x(2).toString, Seq(x(3).toString), Seq(x(4).toString.toDouble)))
			.keyBy(x => (x._1, x._2, x._3))
			.reduceByKey((left, right) => {
				(left._1, left._2, left._3, left._4 ++ right._4, left._5 ++ right._5)
			}).map(x => {
			val marketGrowth = x._2._5(x._2._4.indexOf("total"))
			(x._2._1, x._2._2, x._2._3, x._2._4, x._2._5.map(y => (y + 1) / (marketGrowth + 1)))
		}).toDF("market", "city", "date", "key", "value")
		func_enlarge(aggregateEITemp)
	}

	def getAggregate(chcDF: DataFrame): DataFrame = {
		val func_quaterDF: (DataFrame, String) => DataFrame = (quaterSalesDF, keyType) => {
			//share
			val oadShareDF = aggregateShare(quaterSalesDF)
			//growth
			val oadGrowthDF = aggregateGrowth(quaterSalesDF)
			//shareGrowth
			val oadShareGrowthDF = aggreagteShareGrowth(oadShareDF)
			//EI
			val oadEIDF = aggregateEI(oadGrowthDF)

			val df1 = quaterSalesDF.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("sales"))
			val df2 = oadShareDF.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("share"))
			val df3 = oadGrowthDF.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("growth"))
			val df4 = oadShareGrowthDF.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("shareGrowth"))
			val df5 = oadEIDF.withColumn("keyType", lit(keyType))
				.withColumn("valueType", lit("EI"))
			df1.union(df2).union(df3).union(df4).union(df5)
		}

		val func_YTDDF: (DataFrame, String) => DataFrame = (quaterSalesDF, keyType) => {
			//YTD Sales
			val YTDSalesDF = aggregateYTDSales(quaterSalesDF)
			func_quaterDF(YTDSalesDF, keyType)
		}

		val salesMap = aggregateSales(chcDF)
		//sales
		val oadSalesDF = salesMap("oadSales")
		val moleSalesDF = salesMap("moleSales")
		val prodSalesDF = salesMap("prodSales")
		val mnfSalesDF = salesMap("mnfSales")

		val func_YTDDate: UserDefinedFunction = udf{
			date: String => date + "YTD"
		}

		val quaterData = Map("oad" -> oadSalesDF, "mole" -> moleSalesDF, "prod" -> prodSalesDF, "mnf" -> mnfSalesDF)
		val quaterResultlst = quaterData.map(x => func_quaterDF(x._2, x._1))
		val quaterResult = quaterResultlst.tail.foldLeft(quaterResultlst.head)((x1, x2) => x1.union(x2))
		val YTDResultlst = quaterData.map(x => func_YTDDF(x._2, x._1))
		val YTDResult = YTDResultlst.tail.foldLeft(YTDResultlst.head)((x1, x2) => x1.union(x2))
			.withColumn("date", func_YTDDate(col("date")))
		quaterResult.union(YTDResult)
	}
}

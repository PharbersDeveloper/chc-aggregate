package com.pharbers.aggregate

import org.apache.spark.sql.functions._
import com.pharbers.aggregate.common.phFactory
import com.pharbers.aggregate.moudle.aggredData
import com.pharbers.spark.util.readCsv
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType

object main extends App with Serializable {
//	val sprakDriver = phFactory.getSparkInstance()
//
//	import sprakDriver.conn_instance
//
//	import sprakDriver.ss.implicits._
//
//	val chcDF = sprakDriver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
//
//	val func_castTime: UserDefinedFunction = udf {
//		str: String => {
//			val ymStr = str.split("Q")
//			val year = ymStr.head
//			val month = ymStr.last.toInt
//			if (month < 10) {
//				year.toString + "0" + month.toString
//			} else {
//				year.toString + month.toString
//			}
//		}
//	}
//
//	val chcTempDF = chcDF.select("city", "Date", "OAD类别", "Molecule_Desc", "分子", "Prod_Desc", "MNF_Desc", "Sales")
//		.na.fill("")
//		.withColumn("Sales", col("Sales").cast(DoubleType))
//
//	val func_group: (DataFrame, List[String], String) => DataFrame = (df, groupKeyList, sumCol) => {
//		df.groupBy(groupKeyList.head, groupKeyList.tail: _*)
//			.sum(sumCol)
//			.withColumnRenamed("sum(" + sumCol + ")", sumCol)
//	}
//
//	val oadDF = func_group(chcTempDF, List("city", "Date", "OAD类别"), "Sales")
//	val moleDF = func_group(chcTempDF, List("city", "Date", "分子"), "Sales")
//	val prodDF = func_group(chcTempDF, List("city", "Date", "Prod_Desc"), "Sales")
//	val mnfDF = func_group(chcTempDF, List("city", "Date", "MNF_Desc"), "Sales")
//
//	val result = oadDF.toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString, Seq(x(2).toString), Seq(x(3).toString.toDouble)))
//		.keyBy(x => (x._1, x._2))
//		.reduceByKey((left, right) => {
//			(left._1, left._2, left._3 ++ right._3, left._4 ++ right._4)
//		}).map(x => {
//		val totalResult = x._2._4.sum
//		(x._1._1, x._1._2, x._2._3, x._2._4.map(sales => sales / totalResult))
//	}).toDF("city", "Date", "OAD类别", "Sales")
//
//	result.show(false)
//
//	val result1 = result.flatMap(x => {
//		val oadArray = x.getAs[Seq[String]](2)
//		val salesArray = x.getAs[Seq[Double]](3)
//		oadArray.map(oad => aggredData(x.getString(0), x.getString(1), oad, salesArray(oadArray.indexOf(oad))))
//	})
//	result1.show(false)
//
//	result1.toDF().sort("city", "Date").show(false)





	//	val func_zip: UserDefinedFunction = udf{
	//		(oadType: Array[String], salesArray: Array[Double]) => oadType.map(x => x + 31.toChar.toString + salesArray(oadType.indexOf(x)).toString)
	//	}
	//	val result1 = result.withColumn("oadTemp", func_zip(col("OAD类别"), col("Sales")))
	//		.withColumn("test", explode(col("oadTemp")))
	//	result1.show(false)


	//	val sumOadDF = func_group(oadDF, List("city", "Date"), "Sales")
	//		.withColumn("OAD类别", lit("total"))
	//		.select("city", "Date", "OAD类别", "Sales")
	//		.union(oadDF)
	//	val sumMoleDF = func_group(moleDF, List("city", "Date"), "Sales")
	//		.withColumn("分子", lit("total"))
	//		.select("city", "Date", "分子", "Sales")
	//		.union(moleDF)
	//	val sumProdDF = func_group(prodDF, List("city", "Date"), "Sales")
	//		.withColumn("Prod_Desc", lit("total"))
	//		.select("city", "Date", "Prod_Desc", "Sales")
	//		.union(prodDF)
	//	val sumMnfDF = func_group(mnfDF, List("city", "Date"), "Sales")
	//		.withColumn("MNF_Desc", lit("total"))
	//		.select("city", "Date", "MNF_Desc", "Sales")
	//		.union(mnfDF)
	//
	//	import org.apache.spark._
	//
	//	val result = sumOadDF.groupByKey(x => (x(0).toString, x(1).toString))
	//		.reduceGroups((left, right) => {
	//			Row(left(0).toString, left(1).toString, Array(left(2).toString, right(2).toString), Array(left(3).toString.toDouble, right(3).toString.toDouble))
	//		})
	//
	//	result.show(false)
	//	result.show(false)
	//	println("oadDF")
	//	sumOadDF.show(false)
	//	println("moleDF")
	//	sumMoleDF.show(false)
	//	println("prodDF")
	//	sumProdDF.show(false)
	//	println("mnfDF")
	//	sumMnfDF.show(false)
}

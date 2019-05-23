package com.pharbers.aggregate

import com.pharbers.aggregate.ppt.aggregateData
import com.pharbers.aggregate.util.saveDF2mongo
import com.pharbers.data.conversion.{CHCConversion, ProductDevConversion, ProductImsConversion}
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}
import org.apache.spark.sql.DataFrame
import com.pharbers.data.util._
import com.pharbers.data.util.ParquetLocation._
import org.apache.spark.sql.functions.lit

object main extends App with Serializable {
	//	val chcFile1 = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
	val ohaFile = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"
	val caFile = "/test/CHC_CA_7cities_Delivery.csv"
	val piCvs = ProductImsConversion()
	val chcCvs = CHCConversion()

	val productImsDIS_ca = piCvs.toDIS(MapArgs(Map(
		"productImsERD" -> DFArgs(Mongo2DF(PROD_IMS_LOCATION.split("/").last))
		//		, "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
		//		, "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
		, "productDevERD" -> DFArgs(Mongo2DF("prod_dev9"))
	))).getAs[DFArgs]("productImsDIS")

	val productImsDIS_oha = piCvs.toDIS(MapArgs(Map(
		"productImsERD" -> DFArgs(Mongo2DF(PROD_IMS_LOCATION.split("/").last))
		, "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
		, "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
		, "productDevERD" -> DFArgs(Mongo2DF("prod_dev9"))
	))).getAs[DFArgs]("productImsDIS")

	def getchcDF(productImsDIS: DataFrame, filePaht: String): DataFrame = {
		val chcDF = CSV2DF(filePaht)
		//		val chcDFCount = chcDF.count()
		val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
		val chcERD = chcCvs.toERD(MapArgs(Map(
			"chcDF" -> DFArgs(chcDF)
			, "dateDF" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
			, "cityDF" -> DFArgs(cityDF)
			, "productDIS" -> DFArgs(productImsDIS)
			, "addCHCProdFunc" -> SingleArgFuncArgs { df: DataFrame =>
				ProductDevConversion().toERD(MapArgs(Map(
					"chcDF" -> DFArgs(df)
				))).getAs[DFArgs]("productDevERD")
			}
		))).getAs[DFArgs]("chcERD")
		val chcDIS = chcCvs.toDIS(MapArgs(Map(
			//			"chcERD" -> DFArgs(Mongo2DF(CHC_LOCATION.split("/").last))
			"chcERD" -> DFArgs(chcERD)
			, "dateERD" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
			, "cityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
			, "productDIS" -> DFArgs(productImsDIS)
		))).getAs[DFArgs]("chcDIS")
		chcDIS
	}

	val caDF = getchcDF(productImsDIS_ca, caFile).withColumn("MARKET", lit("钙补充剂市场"))
	val ohaDF = getchcDF(productImsDIS_oha, ohaFile).withColumn("MARKET", lit("口服降糖药市场"))
	val chcDIS = caDF.unionByName(ohaDF)
	val aggreagteDFTemp = aggregateData().getAggregate(chcDIS)
	saveDF2mongo().saveDF(aggreagteDFTemp, "aggregateData")
}

package com.pharbers

import com.pharbers.aggregate.common.phFactory
import com.pharbers.aggregate.ppt.aggregateForPPT
import com.pharbers.spark.util.readCsv
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

object aggregateTest extends App {
	val sprakDriver = phFactory.getSparkInstance()

	import sprakDriver.conn_instance

	import sprakDriver.ss.implicits._

	val chcDF = sprakDriver.setUtil(readCsv()).readCsv("/test/OAD CHC data for 5 cities to 2018Q3 v3.csv")
		.select("city", "Date", "OAD类别", "Molecule_Desc", "分子", "Prod_Desc", "MNF_Desc", "Sales")
		.na.fill("")
		.withColumn("Sales", col("Sales").cast(DoubleType))
    	.withColumn("market", lit("降糖药市场"))

	val aggrate = aggregateForPPT()
	//Sales
	val salesMap = aggrate.aggregateSales(chcDF)

	val oadSalesDF = salesMap("oadSales")
	oadSalesDF.show(false)
	val moleSalesDF = salesMap("moleSales")
	moleSalesDF.show(false)
	val prodSalesDF = salesMap("prodSales")
	prodSalesDF.show(false)
	val mnfSalesDF = salesMap("mnfSales")
	mnfSalesDF.show(false)

	//YTD Sales
	val oadYTDSalesDF = aggrate.aggregateYTDSales(oadSalesDF)
	oadYTDSalesDF.show(false)
	val moleYTDSalesDF = aggrate.aggregateYTDSales(moleSalesDF)
	moleYTDSalesDF.show(false)
	val prodYTDSalesDF = aggrate.aggregateYTDSales(prodSalesDF)
	prodYTDSalesDF.show(false)
	val mnfYTDSalesDF = aggrate.aggregateYTDSales(mnfSalesDF)
	mnfYTDSalesDF.show(false)

	//YTD Share
	val oadYTDShareDF = aggrate.aggregateShare(oadYTDSalesDF)
	oadYTDShareDF.show(false)
	val moleYTDShareDF = aggrate.aggregateShare(moleYTDSalesDF)
	moleYTDShareDF.show(false)
	val prodYTDShareDF = aggrate.aggregateShare(prodYTDSalesDF)
	prodYTDShareDF.show(false)
	val mnfYTDShtareDF = aggrate.aggregateShare(mnfYTDSalesDF)
	mnfYTDShtareDF.show(false)

	//YTD Growth
	val oadYTDGrowthDF = aggrate.aggregateGrowth(oadYTDSalesDF)
	oadYTDGrowthDF.show(false)
	val moleYTDGrowthDF = aggrate.aggregateGrowth(moleYTDSalesDF)
	moleYTDGrowthDF.show(false)
	val prodYTDGrowthDF = aggrate.aggregateGrowth(prodYTDSalesDF)
	prodYTDGrowthDF.show(false)
	val mnfYTDGrowthDF = aggrate.aggregateGrowth(mnfYTDSalesDF)
	mnfYTDGrowthDF.show(false)

	//YTD ShareGrowth
	val prodYTDShareGrowth = aggrate.aggreagteShareGrowth(prodYTDShareDF)
	prodYTDShareGrowth.show(false)

	//YTD EI
	val prodYTDEI = aggrate.aggregateEI(prodYTDGrowthDF)
	prodYTDEI.show(false)
	println(prodYTDEI.count())




	//Share
	val oadShareDF = aggrate.aggregateShare(oadSalesDF)
	oadShareDF.show(false)
	val moleShareDF = aggrate.aggregateShare(moleSalesDF)
	moleShareDF.show(false)
	val prodShareDF = aggrate.aggregateShare(prodSalesDF)
	prodShareDF.show(false)
	val mnfShtareDF = aggrate.aggregateShare(mnfSalesDF)
	mnfShtareDF.show(false)

	//Growth
	val oadGrowthDF = aggrate.aggregateGrowth(oadSalesDF)
	oadGrowthDF.show(false)
	val moleGrowthDF = aggrate.aggregateGrowth(oadSalesDF)
	moleGrowthDF.show(false)
	val prodGrowthDF = aggrate.aggregateGrowth(prodSalesDF)
	prodGrowthDF.show(false)
	val mnfGrowthDF = aggrate.aggregateGrowth(mnfSalesDF)
	mnfGrowthDF.show(false)
}

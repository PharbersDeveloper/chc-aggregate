package com.pharbers.ahcaggregate.util

import com.mongodb.DBObject
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.pharbers.ahcaggregate.common.{connectMongoInfo, phFactory}
import com.pharbers.spark.util.dataFrame2Mongo
import org.apache.spark.sql.DataFrame

case class saveDF2mongo() extends connectMongoInfo{
	private lazy val phSparkDriver = phFactory.getSparkInstance()

	import phSparkDriver.conn_instance

	def saveDF(df: DataFrame, tableName: String): Unit ={
		phSparkDriver.setUtil(dataFrame2Mongo()).dataFrame2Mongo(df, server_host, server_port.toString,
			conn_name, tableName)
	}
}

case class save2Mongo() extends connectMongoInfo{
	def save(value: DBObject): Unit = {
		MongoClient(server_host, server_port).getDB(conn_name).getCollection("chc-ppt").save(value)
	}
}

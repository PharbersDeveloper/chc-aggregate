package com.pharbers.ahcaggregate.common

import com.pharbers.spark.phSparkDriver

object phFactory {
	private lazy val sparkDriver: phSparkDriver = phSparkDriver("chc-spark-driver")

	def getSparkInstance(): phSparkDriver = {
		sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/spark_driver-1.0.jar")
		sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/mongo-java-driver-3.8.0.jar")
		sparkDriver.sc.addJar("/Users/cui/github/chc-aggregate/target/pharbers-chc-aggregate-1.0-SNAPSHOT.jar")
		sparkDriver
	}

}

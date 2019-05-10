package com.pharbers.aggregate.common

import com.pharbers.spark.phSparkDriver
import scala.reflect.runtime.universe

object phFactory {
	private lazy val sparkDriver: phSparkDriver = phSparkDriver("chc-spark-driver")

	def getSparkInstance(): phSparkDriver = {
		sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/spark_driver-1.0.jar")
		sparkDriver.sc.addJar("hdfs:///jars/phDataRepository/mongo-java-driver-3.8.0.jar")
		sparkDriver.sc.addJar("/Users/cui/github/chc-aggregate/target/pharbers-chc-aggregate-1.0-SNAPSHOT.jar")
		sparkDriver
	}

	def getInstance(name: String): Any = {
		println(s"create instance for $name")
		val m = universe.runtimeMirror(getClass.getClassLoader)
		val clssyb = m.classSymbol(Class.forName(name))
		val cm = m.reflectClass(clssyb)
		val ctor = clssyb.toType.decl(universe.termNames.CONSTRUCTOR).asMethod
		val ctorm = cm.reflectConstructor(ctor)
		val tmp = ctorm()
		tmp
	}
}

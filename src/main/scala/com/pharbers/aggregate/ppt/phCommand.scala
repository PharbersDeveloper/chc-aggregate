package com.pharbers.aggregate.ppt

import com.pharbers.aggregate.moudle.pptInputData
import org.apache.spark.sql.DataFrame

trait phCommand {
	def exec(dataObj: pptInputData, df: DataFrame) : Unit
}

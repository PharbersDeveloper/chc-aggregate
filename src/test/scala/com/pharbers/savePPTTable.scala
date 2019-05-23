package com.pharbers

import com.pharbers.aggregate.moudle.marketTables.{caTables, ohaTables}
import com.pharbers.data.util.Mongo2DF
import com.pharbers.aggregate.common.phFactory
import com.pharbers.aggregate.ppt.phCommand

object savePPTTable extends App {
	val aggreagteDF = Mongo2DF("aggregateData")
	val caTableList = caTables().getAllTableList()
	val ohaTableList = ohaTables().getAllTableList()
	val allTableList = caTableList ++ ohaTableList
	allTableList.foreach(x => {
		phFactory.getInstance(x.factory).asInstanceOf[phCommand].exec(x, aggreagteDF)
		println(x.tableIndex + "======== 生成完成")
	})
}

package com.pharbers.ahcaggregate.util

object timeUtil {
	def getStartYQ(timeline: String): String = {
		val ymMap: Map[String, Int] = getTimeLineYm(timeline)
		val month = ymMap("month")
		val year = ymMap("year")
		val ymcount = timelineYmCount(timeline)
		getymlst(month, year, ymcount - 1)
	}

	def getymlst(month: Int, year: Int, ymcount: Int): String = {
		if (ymcount == 0) {
			if (month < 10) {
				year.toString + "0" + month.toString
			} else {
				year.toString + month.toString
			}
		} else {
			if (month == 1) getymlst(12, year - 1, ymcount - 1)
			else getymlst(month - 1, year, ymcount - 1)
		}
	}

	//计算timeline需要前推多少个月份
	def timelineYmCount(timeline: String): Int = {
		val month = getTimeLineYm(timeline)("month")
		timeline.split(" ").length match {
			case 3 => timeline.split(" ")(0) match {
				case "MAT" => 12
				case "YTD" => month
				case "RQ" => 3
				case "QTR" => 3
			}
			case 2 => timeline.charAt(0) match {
				case 'M' => 1
				case 'R' => 3
				case 'Q' => 1
			}
		}
	}

	def getTimeLineYm(timeline: String): Map[String, Int] = {
		val ym = timeline.takeRight(5).split(" ")
		val month = ym.head.toInt
		val year = 2000 + ym.last.toInt
		Map("month" -> month, "year" -> year)
	}

	def getAllTimeline(timelineList: List[String]): List[String] = {
		val resultList = timelineList ::: timelineList.map { timeline =>
			val lastYear = (timeline.split(" ").last.toInt - 1).toString
			val lastTimeLine = (timeline.split(" ").take(timeline.split(" ").length - 1) ++ Array(lastYear)).mkString(" ")
			lastTimeLine
		}
		resultList.distinct
	}

	def getAllym(year: Int, month: Int, forward: Int, lst: List[String]): List[String] = {
		if (forward == 0) {
			lst
		}
		else {
			val ym = if (month < 10) {
				year.toString + "0" + month.toString
			} else {
				year.toString + month.toString
			}
			if (month == 1) getAllym(year - 1, 12, forward - 1, lst ::: List(ym))
			else getAllym(year, month - 1, forward - 1, lst ::: List(ym))
		}
	}
}

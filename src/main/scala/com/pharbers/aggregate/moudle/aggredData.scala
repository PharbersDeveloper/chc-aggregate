package com.pharbers.aggregate.moudle

case class aggredData(
                     market: String,
                     city: String,
                     date: String,
                     key: String,
                     value: Double
                     ) extends Serializable {

}

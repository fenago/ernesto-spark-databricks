package training

object recordParser {

  case class records(userIt: Int, movieId: Int, rating: Double, timeStamp: String)

  def parse(record:String): Either[String, records] = {

    val fields: Array[String] = record.split(',')

    if (fields.length == 4) {

      val userId: Int = fields(0).toInt
      val movieId: Int = fields(1).toInt
      val rating: Double = fields(2).toDouble
      val timeStamp: String = fields(3)

      Right(records(userId, movieId, rating, timeStamp))
    }

    else {
      Left(record)
    }

  }
}

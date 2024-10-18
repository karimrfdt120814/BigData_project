object patternMatching {
  def main(args:Array[String]):Unit= {

    val day = "Saturday"

    val result = day match {
      case "Monday" => "Start of week"
      case "Tuesday" | "Wednesday" | "Thursday" => "Mid week"
      case "Friday" => "End of working week"
      case "Saturday" | "Sunday" => s"$day is weekend Enjoy your day"
      case _ => "invalid_day"
    }
    println(s"$result")
  }

}

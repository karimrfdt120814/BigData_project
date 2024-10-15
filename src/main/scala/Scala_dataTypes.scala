object Scala_dataTypes {

  def main(args:Array[String]):Unit={
    val number: Int = 42
    val bigNumber:Long= 1234567890L
    val pi: Double = 3.14159
    val weight: Float = 65.5f
    val isTrue: Boolean = true
    val firstLetter:Char ='A'
    val message: String = "Hello, Scala!"

    val s1:Option[String]=Some("Karimulla")
    val s2:Option[String] = None

    s1.map(_.length)
    s2.map(_.length)

    

  }

}

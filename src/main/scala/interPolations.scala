object interPolations {

  def main(args: Array[String]):Unit={

    val name = "James"
    val age = 30.5

    println(s"his name is $name and his age is $age")

    println(s"2 + 2 = ${2 + 2}") // "2 + 2 = 4"
    val x = -1
    println(s"x.abs = ${x.abs}") // "x.abs = 1"

    println(s"2+2 =${2+2}")

    println(s"""{"name":"James"}""")     // `{"name":"James"}`

    val height = 1.9d
    val nam  = "James"
    println(f"$nam%s is $height%2.2f meters tall")

    println(s"""{"name":"Karim}"""")
  }

}

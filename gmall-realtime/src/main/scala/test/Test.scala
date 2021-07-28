package test

object Test {
  def main(args: Array[String]): Unit = {
    val str: String = null
    var op = "unUpdate"
    str match {
      case null =>
      case "c" =>
        op = "create"
      case "u" =>
        op = "update"
      case "d" =>
        op = "delete"
    }

    println(op)
  }
}

package org.example.youtubeCourse

object Basics extends App {

  // definiendo una variable
  val meaningOfLife: Int = 42 // int invariable (val)

  // Int, Boolean, Char, Double, Float, String
  val aBoolean = false // la tipación es opcional ya que se reconoce automáticamente

  val aString = "This is a String"
  val aComposedString = "This" + " is " + " a " + " composed " + " String"
  val str = "String"
  val anInterpolatedstring = s"This is an interpolated " + str

  // las expresiones son estructuras que pueden reducirse a valores
  val anExpression = 2 + 3

  // if-expression
  val ifExpression = if (meaningOfLife > 43) 56 else 999
  val chaindeIfExpression =
    if (meaningOfLife > 43) 56
    else if (meaningOfLife < 0) -2
    else if (meaningOfLife > 999) 78
    else 0

  // Code blocks
  val aCodeBlock = {
    // Definitions
    val aLocalValue = 67

    // Value of block is the value of the las expression
    aLocalValue + 3
  }

  // Defining a function
  def myFunction(x: Int, y:String): String = {
    y + " " + x
  }

  // Recursive functions
  def factorial(n: Int): Int =
    if (n <= 1) 1
    else n * factorial(n - 1)


  // In Scala we don't use loops or iterational, we use RECURSION!!!

  // The Unit type = no meaningful value === "void" in other lenguages
  // Type of SIDE EFFECTS
  println("I love Scala") // System.out.println, printf, print, console.log

  def myUnitReturningFunction(): Unit = {
    println("I don't love returning Unit")
  }

  val theUnit = ()

}

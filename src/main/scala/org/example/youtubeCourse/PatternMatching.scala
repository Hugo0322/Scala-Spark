package org.example.youtubeCourse

object PatternMatching extends App {

  // Switch expression
  val anInteger = 55
  val order = anInteger match {
    case 1 => "first"
    case 2 => "second"
    case 3 => "third"
    case _ => anInteger + "th"
  }
  // PatternMatch is an EXPRESSION
  println(order)

  // Class decomposition
  case class Person(name: String, age: Int)

  val bob = Person("Bob", 22) // Person.apply("Bob", 22)

  val personGreeting = bob match {
    case Person(n, a) => s"Hi, my name is $n and I'm $a years old"
    case _ => "Someting else"
  }
  println(bob)
  println(personGreeting)

  // Deconstructing tuple
  val aTuple = ("Hello Mate", "Rock")
  val deconstructor = aTuple match {
    case (st, nd) => s"$st belongs to $nd"
    case _ => "I don't know what you are talking about"
  }

  // Decomposing lists
  val aList = List(1,2,3)
  val listDescription = aList match {
    case List(_, 2, _) => "List containing 2 on its second position"
    case _ => "Unknown list"
  }
  // If PatternMatcher doesn't match anything, it will throw a MatchError
  // PM will try all cases in sequence

  // PM is way more powerful than this simple examples

}

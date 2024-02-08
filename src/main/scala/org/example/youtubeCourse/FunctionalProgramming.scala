package org.example.youtubeCourse

object FunctionalProgramming extends App {

  // RECAP
  // Scala is ObjectOrientated
  class Person(name: String) {
    def apply(age: Int) = println(s"I have aged $age years")
  }

  val bob = new Person("Bob")
  bob.apply(43)
  bob(43) // INVOKING bob as a function === bob.apply(43)

  /*
    Scala runs on the JVM
    Functional programming:
    - Compose functions
    - Pass functions as args
    - Return functions as results

    Conclusion: FuncitonX = Function1, Function2 ... Function22
   */

  val simpleIncrementer = new Function1[Int, Int] {
    override def apply(arg: Int): Int = arg + 1
  }

  simpleIncrementer.apply(23) // Returns 24
  simpleIncrementer(23) // simpleIncrementer.apply(23)
  // We just defined a funcion

  // ALL SCALA FUNCTIONS ARE INSTANCES OF THIS FUNCTION_X TYPES

  val stringConcatenator = new Function2[String, String, String] {
    override def apply(arg1: String, arg2: String): String = arg1 + arg2
  }
  stringConcatenator("I love", "Scala") //Returns "I love Scala"

  // Syntax sugars
  val doubler: Function1[Int, Int] = (x: Int) => 2 * x
  doubler(4) // Returns 8

  /*
    new Function1[Int, Int] {
      override def apply(x: Int) = 2 * x
    }
   */

  // Higher-order functions: take functions as args/return functions as result
  val aMappedList = List(1, 2, 3).map(x => x + 1) // Higher order function
  // println(aMappedList)
  val aFlatMappedList = List(1, 2, 3).flatMap(x => List(x, 2 * x))
  // println(aFlatMappedList)
  val aFilteredList = List(1,2,3,4,5).filter(_ <= 3) // Equals (x => x <= 3)
  // println(aFilteredList)

  // All pairs between the numbers 1,2,3 and the letters 'a','b','c'
  val allPairs = List(1,2,3).flatMap(number => List('a','b','c').map(letter => s"$number-$letter"))
  // println(allPairs)

  // For comprehensions
  val alternativePairs = for {
    number <- List(1,2,3)
    letter <- List('a','b','c')
  } yield s"$number-$letter"
  // Equivalent to the map/flatmap chain above

  /**
    Collections
   */

  // Lists
  val aList = List(1,2,3,4,5)
  val firstElement = aList.head
  val rest = aList.tail
  val aPrependedList = 0 :: aList // List(0,1,2,3,4,5)
  val anExtendedList = 0 +: aList :+ 6 // List(0,1,2,3,4,5,6)

  // Sequences
  val aSequence: Seq[Int] = Seq(1,2,3) // Seq.apply(1,2,3)
  val accessedElement = aSequence(1) // Returns the element at index 1: 2

  // Vectors: fast Seq implementation
  val aVector = Vector(1,2,3,4,5)

  // Sets = no duplicates
  val aSet = Set(1,2,3,4,1,2,3) // Set(1,2,3,4)
  val setHas5 = aSet.contains(5) // flase
  val anAddedSet = aSet + 5 // Set(1,2,3,4,5)
  val aRemovedSet = aSet - 3 // Set(1,2,4,5)

  // Ranges
  val aRange = 1 to 1000
  val twoBytwo = aRange.map(x => 2 * x).toList // List(2,4,6...,2000)

  // Tuples = groups of values under the same value
  val aTuple = ("Bon Jovi", "Rock", 1982)

  // Maps
  val aPhoneBook: Map[String, Int] = Map(
    ("Daniel", 1213123),
    "Jane" -> 151551 // ("Jane", 151551)
  )

}

package org.example.youtubeCourse

object ContextualAbstraction {

  /*
    1 -- Context parameters/arguments
   */
  val aList = List(2,1,3,4)
  val anOrderedList = aList.sorted // contextual argument: (ordering)

  // Ordering
  val descendingOrdering: Ordering[Int] = Ordering.fromLessThan(_ > _) // (a,b) => a > b
  // given descendingOrdering: Ordering[Int] = Ordering.fromLessThan(_ > _) // (a,b) => a > b

  // Analogous to an implicit val

  trait Combinator[A] {
    def combine(x: A, y: A): A
  }

  def combineAll[A](list: List[A])(/*using*/ combinator: Combinator[A]): A = ???

  def main(args: Array[String]): Unit = {
    println(anOrderedList)
  }

}

package org.example.youtubeCourse

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

object Advanced extends App {

  // Lazy evaluation
  lazy val aLazyValue = 2
  lazy val lazyValueWithSideEffects = {
    println("I'm so very lazu!")
    43
  }

  val eagerValue = lazyValueWithSideEffects + 1
  // Its useful in infinite collections

  // "Pseudo-collections": Option, Try
  def methodWichCanReturnNull(): String = "Hello, Scala"
  if (methodWichCanReturnNull() == null) {
    // Deffensive code against the null exception expected
  }
  val anOption = Option(methodWichCanReturnNull()) // Some("Hello, Scala")
  // Option = "collection" which contains at most one element // Some(value) or None

  val stringProcessing = anOption match {
    case Some(string) => s"I have obtained a valid String: $string"
    case _ => "I obtained nothing"
  }
  // Map, FlatMap, Filter

  def methodWichCanThrowException(): String = throw new RuntimeException
  try {
    methodWichCanReturnNull()
  } catch {
    case e: Exception => "Defend agains this Exception"
  }
  val aTry = Try(methodWichCanThrowException())
  // a try = "collection" with either a value if the code went well, or an exception if the code threw one

  val anotherStringProcessing = aTry match {
    case Success(string) => s"I have obtained a valid String: $string"
    case Failure(exception) => s"I have obtained an exception: $exception"
  }
  // Map, FlatMap, Filter

  /**
   * Evaluate something on another thread
   * (asynchronous programming)
   */
  val future = Future({
    println("Loading...")
    Thread.sleep(1000)
    println("I have computed a value")
    67
  })

  // Future is a "collection" which contains a value when it's evaluated
  // Future is composable with map, flatmap and filter

  /**
   * Implicits basics
   */
  // 1#: Implicit arguments
  def aMethodWithImplicitArgs(implicit arg: Int) = arg + 1
  implicit val myImplicitInt = 46
  println(aMethodWithImplicitArgs) // aMethodWithImplicitArgs(myImplicitInt)

  // 2#: Implicit conversions
  implicit class MyRichInteger(n: Int) {
    def isEven() = n % 2 == 0
  }

  println(23.isEven()) // new MyRichInteger(23).isEven()


}

package org.example.youtubeCourse

// Extending App is like instancing a public static main in java even tho you can actually instance a main on Scala it shelf
object ObjectOrientation extends App {

  // Creamos una clase y la instanciamos
  class Animal {
    val age: Int = 0
    def eat() = println("I'm eating")
  }
  val anAnimal = new Animal

  // Inheritance
  class Dog(val name: String) extends Animal  // Definición de constrictores
    val aDog = new Dog("Lua")

  // Constructor arguments are NOT fields: necesitas poner val antes del argumento del constructor
  aDog.name

  // Polimorfismos de subtipo
  val aDeclaredAnimal: Animal = new Dog("Perrete")
  aDeclaredAnimal.eat() // The moast derived method will be called at runtime: el más derivado, en este caso si se sobreescribe el método en Dog se llamaría a este

  // Abstract class
  abstract class WalkingAnimal {
    val hasLegs = true // By deffault public, can be restricted by using private or protected
    def walk(): Unit
  }

  // "Interface" == ultimate abstract type
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  trait Philosopher {
    def ?!(thought: String): Unit // Valid method name
  }

  // Single-class inheritance, multi-trait "mixing"
  class Crocodile extends Animal with Carnivore with Philosopher {
    override def eat(animal: Animal): Unit = println("I'm eating you, Animal")
    def ?!(thought: String): Unit = println(s"I was thinking: $thought")
  }

  val aCroc = new Crocodile
  aCroc.eat(aDog)
  aCroc eat aDog  // Infix notation = object method argument, only available for methods with ONE argument
  aCroc ?! "What if we could fly?"

  // Operators in scala are actually methods
  val basicMath = 1 + 2
  val anotherBasicMath = 1.+(2) // Equivalent

  // Anonymous classes
  val dinosaur = new Carnivore {
    override def eat(animal: Animal): Unit = println("I'm a dinosaur so I can eat pretty much anything")
  }

  /*
    This is what you are telling to the compiler:

    class Carnivore_Anonymous_34728 extends Carnivore {
      override def eat(animal: Animal): Unit = println("I'm a dinosaur so I can eat pretty much anything"=
    }

    val dinosaur = new Carnivore_Anonymous_35728
   */

  // Singleton object
  object MySingleton { // This is the only instance of MySingleton type
    val mySpecialValue = 53278
    def mySpecialMethod(): Int = 53278
    def apply(x: Int): Int = x + 1
  }

  MySingleton.mySpecialMethod()
  MySingleton.apply(65)
  MySingleton(65) // Equivalent to MySingleton.apply(65)

  object Animal { // companions - companion object
    // Companions can access each other's private fields / methods
    // Singleton Animal and instances of Animal are different things
    val canLiveIndefinitely = false
  }

  val animalsCanLiveForever = Animal.canLiveIndefinitely // "static" fields/methods

  /*
     case classes = lightweight data structures with some boilerplate
     - sensible equals and has code
     - serialization
     - companion with apply
     - pattern matching
   */
  case class Person(name: String, age: Int)

  // May be constructed without new
  val bob = Person("Bob", 54)

  // Exceptions
  try {
    // Code that can throw
    val x: String = null
    x.length
  } catch {
    case e: Exception => "Some faulty error message"
  } finally {
    // Will execute some code no matter what
  }

  // Generics
  abstract class MyList[T] {
    def head: T
    def tail: MyList[T]
  }

  // using a generic with a concrete type
  val aList: List[Int] = List(1, 2, 3) // List.apply(1, 2, 3)
  val first = aList.head
  val rest = aList.tail
  val aStringList: List[String] = List("Hello", "Scala")
  val firstString = aStringList.head
  val restString = aStringList.tail

  // Point #1: In Scala we usually operate with IMMUTABLE values/objects
  // Any modification to an object must return ANOTHER object
  /*
    Benefits:
    1) Worjs miracles in multithreaded/distributed environment
    2) Helps making sense of the code
   */
  val reversedList = aList.reverse // Returns a NEW list

  // Pont #2: Scala is closest to the ObjectOriented ideal
}

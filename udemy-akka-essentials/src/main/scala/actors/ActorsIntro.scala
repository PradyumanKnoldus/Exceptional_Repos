package actors

import akka.actor.{Actor, ActorSystem, Props}

object ActorsIntro extends App {

  // Part - 1 : Actor System
  val actorSystem = ActorSystem("firstActorSystem")
  println(actorSystem.name)

  // Part - 2 : Create Actors
  // Word Counter Actor
  class WordCountActor extends Actor {
    // Internal Data
    private var totalWords = 0

    // Behaviour
    // Receive == PartialFunction[Any, Unit]
    override def receive: Receive = {
      case message: String =>
        println(s"[Word Counter] I have received: $message")
        totalWords += message.split(" ").length
      case msg => println(s"[Words Counter] I cannot  understand ${msg.toString} ")
    }
  }

  // Part - 3 : Instantiate our Actor
  val wordCounter = actorSystem.actorOf(Props[WordCountActor], "wordCounter")
  val anotherWordCounter = actorSystem.actorOf(Props[WordCountActor], "anotherWordCounter")

  // Part - 4 : Communicating with Actor
  wordCounter ! "I am learning Akka and it's pretty damn cool!"
  anotherWordCounter ! "A different message"

  // -------------------------------------------------------------------------------------------------

  // How to instantiate an Actor with constructor arguments
  object Person {
    def props(name: String): Props = Props(new Person(name))
  }

  class Person(name: String) extends Actor {
    override def receive: Receive = {
      case "Hi" => println(s"Hi! My name is $name")
      case _ =>
    }
  }

  val person = actorSystem.actorOf(Person.props("Pradyuman"), "person")

  person ! "Hi"
}

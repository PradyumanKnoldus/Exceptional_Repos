package actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object ActorCapabilities extends App {

  class SimpleActor extends Actor {
    override def receive: Receive = {
      case "Hi!" => context.sender() ! "Hello there !"    // Replying to message
      case message: String => println(s"[${context.self}] I have received: $message")
      case number: Int => println(s"[Simple Actor] I have received a NUMBER: $number")
      case SpecialMessage(contents) => println(s"[Simple Actor] I have received something SPECIAL: $contents")
      case SendMessageToYourself(contents) => self ! contents
      case sayHiTo(ref) => ref ! "Hi!"
      case wirelessPhoneMessage(content, ref) => ref forward(content + "s")  // Keep the original sender of WPM
    }
  }

  val system = ActorSystem("actorCapabilitiesDemo")
  val simpleActor = system.actorOf(Props[SimpleActor], "simpleActor")

  simpleActor ! "Hello, Actor"

  // 1 - Messages can be of Any type
  // a) Messages must be IMMUTABLE
  // b) Messages must be SERIALIZABLE
  simpleActor ! 42

  case class SpecialMessage(contents: String)
  simpleActor ! SpecialMessage("Some Special Content")

  // 2 - Actors have information about their context and about themselves
  // context.self == this keyword
  case class SendMessageToYourself(contents: String)
  simpleActor ! SendMessageToYourself("I am an Actor and I am proud of it")

  // 3 - Actors can reply to each other
  val pradyuman = system.actorOf(Props[SimpleActor], "pradyuman")
  val nitin = system.actorOf(Props[SimpleActor], "nitin")

  case class sayHiTo(ref: ActorRef)
  pradyuman ! sayHiTo(nitin)

  // 4 - Dead Letters - Garbage Pool of Akka
  pradyuman ! "Hi!"

  // 5 - Forwarding Messages
  // A is sending message to B and B is sending message to C
  // A -> B -> C
  // forwarding : Sending a message with the ORIGINAL Sender

  case class wirelessPhoneMessage(content: String, ref: ActorRef)
//  pradyuman ! wirelessPhoneMessage("Hi!", nitin)
}
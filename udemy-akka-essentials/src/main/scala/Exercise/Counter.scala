package Exercise

import akka.actor.{Actor, ActorSystem, Props}

object Counter extends App {

  object Counter {
    case object Increment
    case object Decrement
    case object Print
  }

  class Counter extends Actor {
    import Counter._

    var count: Int = 0
    override def receive: Receive = {
      case Increment => count += 1
      case Decrement => count -= 1
      case Print => println(s"Current Value is: $count")
    }
  }

  import Counter._

  val system = ActorSystem("aCounterSystem")
  val aCounterActor = system.actorOf(Props[Counter], "aCountManager")

  (1 to 5).foreach(_ => aCounterActor ! Increment)
  aCounterActor ! Print

  (1 to 3).foreach(_ => aCounterActor ! Decrement)
  aCounterActor ! Print

}

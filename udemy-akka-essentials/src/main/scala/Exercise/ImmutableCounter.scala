package Exercise

import akka.actor.{Actor, ActorSystem, Props}

object ImmutableCounter extends App {

  object Counter {
    case object Increment

    case object Decrement

    case object Print
  }

  class Counter extends Actor {

    import Counter._

    override def receive: Receive = countReceive(0)

    def countReceive(currentCount: Int): Receive = {
      case Increment => context.become(countReceive(currentCount + 1))
      case Decrement => context.become(countReceive(currentCount - 1))
      case Print => println(s"Current Count: $currentCount")
    }
  }

  val system = ActorSystem("aCounterSystem")

  import Counter._
  val counter = system.actorOf(Props[Counter], "counter")

  (1 to 5).foreach(_ => counter ! Increment)
  (1 to 5).foreach(_ => counter ! Increment)
  (1 to 5).foreach(_ => counter ! Increment)
  (1 to 2).foreach(_ => counter ! Decrement)
  (1 to 2).foreach(_ => counter ! Decrement)
  counter ! Print

}

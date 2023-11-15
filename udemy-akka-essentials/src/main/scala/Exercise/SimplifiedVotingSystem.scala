package Exercise

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object SimplifiedVotingSystem extends App {

  object Citizen {
    case class Vote(citizen: String)
    case object VoteStatusRequest
    case class VoteStatusReply(candidate: Option[String])
  }
  class Citizen extends Actor {
    import Citizen._

    override def receive: Receive = {
      case Vote(votedCandidate) => context.become(voted(votedCandidate))
      case VoteStatusRequest => sender() ! VoteStatusReply(None)
    }

    def voted(candidate: String): Receive = {
      case VoteStatusRequest => sender() ! VoteStatusReply(Some(candidate))
    }
  }

  object VoteAggregator {
    case class AggregateVotes(citizen: Set[ActorRef])
  }
  class VoteAggregator extends Actor {
    import Citizen._
    import VoteAggregator._

    override def receive: Receive = awaitingCommand

    def awaitingCommand: Receive = {
      case AggregateVotes(citizens) =>
        citizens.foreach(citizenRef => citizenRef ! VoteStatusRequest)
        context.become(awaitingStatuses(citizens, Map()))
    }

    def awaitingStatuses(stillWaiting: Set[ActorRef], currentStats: Map[String, Int]): Receive = {
      case VoteStatusReply(None) =>
        sender() ! VoteStatusRequest
      case VoteStatusReply(Some(candidate)) =>
        val newStillWaiting = stillWaiting - sender()
        val currentVotesOfCandidate = currentStats.getOrElse(candidate, 0)
        val newStats = currentStats + (candidate -> (currentVotesOfCandidate + 1))
        if (newStillWaiting.isEmpty) {
          println(s"Poll Stats: $newStats")
        } else {
          context.become(awaitingStatuses(newStillWaiting, newStats))
        }
    }
  }

  val system = ActorSystem("SimplifiedVotingSystem")
  val pradyuman = system.actorOf(Props[Citizen])
  val shivam = system.actorOf(Props[Citizen])
  val aman = system.actorOf(Props[Citizen])
  val vibhu = system.actorOf(Props[Citizen])

  import Citizen._
  import VoteAggregator._

  pradyuman ! Vote("BJP")
  shivam ! Vote("Congress")
  aman ! Vote("AAP")
  vibhu ! Vote("BJP")

  val voteAggregator = system.actorOf(Props[VoteAggregator])
  voteAggregator ! AggregateVotes(Set(pradyuman, shivam, aman, vibhu))


}

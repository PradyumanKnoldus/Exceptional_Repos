package Exercise

import Exercise.BankAccount.User.BankingOperations
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

object BankAccount extends App {
  val system = ActorSystem("bankAccountSystem")

  // ----------------------------------------------------------

  object BankAccount {
    case class DepositAmount(amount: Double)
    case class WithdrawAmount(amount: Double)
    case object Statement
    case class TransactionStatus(message: String)
  }

  class BankAccount extends Actor {
    import BankAccount._

    var funds = 18000.0
    override def receive: Receive = {
      case DepositAmount(amount) =>
        if (amount < 0) {
          sender() ! TransactionStatus("Transaction Failed! Invalid Deposit Amount!")
        } else {
          funds += amount
          sender() ! TransactionStatus("Amount Deposited Successfully!")
        }
      case WithdrawAmount(amount) =>
        if (amount > funds || amount < 0) {
          if (amount > funds) sender() ! TransactionStatus("Transaction Failed! Insufficient Amount!")
          else sender() ! TransactionStatus("Transaction Failed! Invalid Withdraw Amount!")
        } else {
          funds -= amount
          sender() ! TransactionStatus("Amount Withdrew Successfully!")
        }
      case Statement => println(s"Current Balance: $funds")
    }
  }

  // ----------------------------------------------------------

  object User {
    case class BankingOperations(account: ActorRef)
  }

  class User extends Actor {
    import BankAccount._
    import User._

    override def receive: Receive = {
      case BankingOperations(account) =>
        account ! DepositAmount(50000)
        account ! WithdrawAmount(10000)
        account ! WithdrawAmount(100000)
        account ! Statement
      case message => println(message.toString)
    }
  }

  // ----------------------------------------------------------

  val bankAccount = system.actorOf(Props[BankAccount], "bankOfKnoldus")
  val user = system.actorOf(Props[User], "pradyuman")

  user ! BankingOperations(bankAccount)



}

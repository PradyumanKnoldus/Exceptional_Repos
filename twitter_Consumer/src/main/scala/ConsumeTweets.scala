package com.knoldus

import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.streaming.StreamingMessage
import com.danielasfregola.twitter4s.entities.{AccessToken, ConsumerToken, Tweet}


object ConsumeTweets extends App {
  val consumerToken = ConsumerToken(key = "7Vx5nxKhOlpwJWS6bqtB98Qnf", secret = "LwEAatzCZ8IyPfMnyiLNEnSJpcytny95ye4hSeF0CLNu4VojTN")
  val accessToken = AccessToken(key = "1693591852878884864-vy2LIc1nzpxEmqTeH7PWgSESFiGhjV", secret = "uk4OqEFmQRCv1cAzkSQnd3VMRMge2wi41Im7WoqMVwcpL")

  //------------------------------------------------------------------------------

  val streamingClient = TwitterStreamingClient(consumerToken, accessToken)

  def printTweetText: PartialFunction[StreamingMessage, Unit] = {
    case tweet: Tweet => println(tweet.text)
  }

  streamingClient.sampleStatuses(stall_warnings = true)(printTweetText)
}

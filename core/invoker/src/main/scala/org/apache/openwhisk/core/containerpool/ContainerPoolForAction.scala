package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, Props}
import akka.stream.scaladsl.Source
import org.apache.openwhisk.core.connector._
import scala.concurrent.Future
import scala.util.Try

object ContainerPoolForAction {
  def props(senderFut: Future[TickerSendEnd], activations: Source[Array[Byte], NotUsed]) =
    Props(new ContainerPoolForAction(senderFut, activations))
}

class ContainerPoolForAction(senderFut: Future[TickerSendEnd], activations: Source[Array[Byte], NotUsed])
    extends Actor {
  override def receive: Receive = Actor.emptyBehavior
}

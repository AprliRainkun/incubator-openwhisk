package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.openwhisk.core.containerpool.ContainerPoolForAction.NewMessage

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object ContainerPoolForAction {
  def props(activations: Source[Array[Byte], NotUsed]) =
    Props(new ContainerPoolForAction(activations))

  final case object NeedMessage

  final case class NewMessage(data: Array[Byte])
}

class ContainerPoolForAction(activations: Source[Array[Byte], NotUsed]) extends Actor {

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = sys.dispatcher

  implicit val timeout: Timeout = Timeout(1 second)
  activations.mapAsyncUnordered(5)(self ? NewMessage(_)).runWith(Sink.ignore)

  override def receive: Receive = {
    case NewMessage(data) =>
      // parse data
      // construct a runnable action by fetching code from database
      // ack immediately on failure
      // dispatch runnable action to container proxy

  }
}

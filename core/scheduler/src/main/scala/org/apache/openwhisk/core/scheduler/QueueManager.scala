package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.DocInfo
import org.apache.openwhisk.grpc.{ActionIdentifier, Activation, WindowAdvertisement}

import scala.concurrent.ExecutionContext

sealed abstract class QueueOperationError
case object QueueNotExist extends QueueOperationError
case class Overloaded(limit: Int) extends QueueOperationError

object QueueManager {
  def props(schedulerConfig: SchedulerConfig)(implicit logging: Logging) = Props(new QueueManager(schedulerConfig))

  final case class CreateQueue(action: DocInfo)
  final case class QueueCreated()

  final case class AppendActivation(activation: Activation)

  type AppendResult = Either[QueueOperationError, Unit]

  final case class EstablishFetchStream(windows: Source[WindowAdvertisement, NotUsed])
  final case class FetchStream(activations: Source[Activation, NotUsed])
  type EstablishResult = Either[QueueOperationError, FetchStream]

  private case class ActionNameDiscovered(action: DocInfo,
                                          windows: Source[WindowAdvertisement, NotUsed],
                                          sender: ActorRef)
}

class QueueManager(schedulerConfig: SchedulerConfig)(implicit logging: Logging) extends Actor {
  import QueueManager._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private var queues = Map.empty[DocInfo, ActorRef]

  override def receive: Receive = {
    case CreateQueue(action) =>
      if (!queues.contains(action)) {
        val queue = context.actorOf(Queue.props(action, schedulerConfig))
        queues = queues + (action -> queue)
      }
      sender ! QueueCreated()
    case msg @ AppendActivation(act) =>
      val actionId = parseDocInfo(act.action.head)
      queues.get(actionId) match {
        case Some(queue) => queue forward msg
        case None =>
          sender ! Left(QueueNotExist)
      }
    case EstablishFetchStream(mixed) =>
      val requester = sender()
      mixed.prefixAndTail(1).runWith(Sink.head) map {
        case (first, remaining) =>
          // todo: warn when actionName is absent (default to "")
          val action = parseDocInfo(first.head.getAction)
          logging.info(this, s"the requested stream is for action $action")
          ActionNameDiscovered(action, remaining, requester)
      } pipeTo self
    case ActionNameDiscovered(action, windows, requester) =>
      queues.get(action) match {
        case Some(queue) => queue.tell(EstablishFetchStream(windows), requester)
        case None        => requester ! Left(QueueNotExist)
      }
  }

  private def parseDocInfo(action: ActionIdentifier) =
    DocInfo ! (action.id, action.revision)
}

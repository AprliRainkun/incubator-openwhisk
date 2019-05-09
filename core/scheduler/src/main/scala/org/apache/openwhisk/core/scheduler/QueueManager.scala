package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Props}
import akka.grpc.GrpcClientSettings
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.grpc.{Activation, WindowAdvertisement}

import scala.concurrent.ExecutionContext

sealed abstract class QueueOperationError
case object QueueNotExist extends QueueOperationError
case object Overloaded extends QueueOperationError

object QueueManager {
  def props(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) =
    Props(new QueueManager(etcdClientConfig, schedulerConfig))

  final case class CreateQueue(name: String)
  final case class QueueCreated()

  final case class AppendActivation(activation: Activation)

  type AppendResult = Either[QueueOperationError, Unit]

  final case class EstablishFetchStream(windows: Source[WindowAdvertisement, NotUsed])
  final case class FetchStream(activations: Source[Activation, NotUsed])
  type EstablishResult = Either[QueueOperationError, FetchStream]

  private case class ActionNameDiscovered(action: String,
                                          windows: Source[WindowAdvertisement, NotUsed],
                                          sender: ActorRef)
}

class QueueManager(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) extends Actor {
  import QueueManager._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private var queues = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case CreateQueue(name) =>
      if (!queues.contains(name)) {
        val queue = context.actorOf(Queue.props(name))
        queues = queues + (name -> queue)
      }
      sender ! QueueCreated()
    case msg @ AppendActivation(act) =>
      queues.get(act.actionName) match {
        case Some(queue) => queue forward msg
        case None =>
          sender ! Left(QueueNotExist)
      }
    case EstablishFetchStream(mixed) =>
      val requester = sender()
      mixed.prefixAndTail(1).runWith(Sink.head) map {
        case (first, remaining) =>
          // todo: warn when actionName is absent (default to "")
          val action = first.head.getActionName
          ActionNameDiscovered(action, remaining, requester)
      } pipeTo self
    case ActionNameDiscovered(action, windows, requester) =>
      queues.get(action) match {
        case Some(queue) => queue.tell(EstablishFetchStream(windows), requester)
        case None        => requester ! Left(QueueNotExist)
      }
  }
}

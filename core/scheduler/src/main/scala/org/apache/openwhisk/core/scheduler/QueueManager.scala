package org.apache.openwhisk.core.scheduler

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Props}
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.grpc.WindowAdvertisement

import scala.concurrent.ExecutionContext

sealed abstract class QueueOperationError
case object QueueNotExist extends QueueOperationError
case object Overloaded extends QueueOperationError

object QueueManager {
  def props(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) =
    Props(new QueueManager(etcdClientConfig, schedulerConfig))

  final case class CreateQueue(name: String)
  final case class QueueCreated()

  final case class AppendActivation(dummyActivation: DummyActivation)

  type AppendResult = Either[QueueOperationError, Unit]

  final case class EstablishFetchStream(windows: Source[WindowAdvertisement, NotUsed])
  final case class FetchStream(activations: Source[DummyActivation, NotUsed])
  type EstablishResult = Either[QueueOperationError, FetchStream]
}

class QueueManager(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) extends Actor {
  import QueueManager._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private var queues = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case CreateQueue(name) =>
      val queue = context.actorOf(Queue.props(name))
      queues = queues + (name -> queue)
      sender ! QueueCreated()
    case msg @ AppendActivation(act) =>
      queues.get(act.action) match {
        case Some(queue) => queue forward msg
        case None =>
          sender ! Left(QueueNotExist)
      }
    case EstablishFetchStream(windows) =>
      val currentSender = sender()

      windows.prefixAndTail(1).toMat(Sink.head)(Keep.right).run() map {
        case (first, remaining) =>
          // todo: warn when actionName is absent (default to "")
          val name = first.head.getActionName
          // todo: race?
          queues.get(name) match {
            case Some(queue) => queue forward EstablishFetchStream(remaining)
            case None        => currentSender ! Left(QueueNotExist)
          }
      }

  }
}

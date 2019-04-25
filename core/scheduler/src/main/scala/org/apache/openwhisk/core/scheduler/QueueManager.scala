package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, ActorRef, Props}
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}

import scala.concurrent.ExecutionContext

object QueueManager {
  def props(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) =
    Props(new QueueManager(etcdClientConfig, schedulerConfig))

  final case class CreateQueue(name: String)
  final case class QueueCreated()

  final case class AppendActivation(dummyActivation: DummyActivation)

  sealed abstract class AppendResponse
  final case object Succeed extends AppendResponse
  final case object QueueNotExist extends AppendResponse
  final case object QueueTooLong extends AppendResponse

  final case class FetchActivation(name: String, window: Int)

}

class QueueManager(etcdClientConfig: GrpcClientSettings, schedulerConfig: SchedulerConfig) extends Actor {
  import QueueManager._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.system.dispatcher

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
          sender ! QueueNotExist
      }
  }
}

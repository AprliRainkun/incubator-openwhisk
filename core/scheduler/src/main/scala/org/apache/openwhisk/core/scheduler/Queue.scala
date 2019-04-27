package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.stream.scaladsl.Flow
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Queue {
  def props(name: String) = Props(new Queue(name))

  final case object RegisterStage
  final case object FetchRequest
  final case class Fetched(activation: DummyActivation)
}

class Queue(name: String) extends Actor {
  import Queue._
  import QueueManager._

  implicit val ex: ExecutionContext = context.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  private var queue = immutable.Queue.empty[DummyActivation]
  private var fetchers = Map.empty[ActorRef, Int]

  override def receive: Receive = {
    case AppendActivation(act) =>
      queue = queue.enqueue(act)
      sender ! Right(Unit)
      trySendActivation(None)
    case EstablishFetchStream(windows) =>
      implicit val timeout: Timeout = Timeout(5.seconds)

      val flow = Flow.fromGraph(new ActivationStage(self))
      val stream = windows.map(w => w.getWindowsSize).via(flow)
      sender ! FetchStream(stream)
    case RegisterStage =>
      fetchers += (sender -> 0)
      context.watch(sender)
    case FetchRequest =>
      val oldVal = fetchers(sender)
      fetchers += (sender -> (oldVal + 1))
      trySendActivation(Some(sender))
    case Terminated(f) =>
      fetchers -= f
  }

  private def trySendActivation(hint: Option[ActorRef]): Unit = {
    hint orElse {
      fetchers collectFirst {
        case (f, window) if window > 0 => f
      }
    } foreach { fetcher =>
      if (queue.nonEmpty) {
        val (elem, newQueue) = queue.dequeue
        val oldVal = fetchers(fetcher)
        queue = newQueue
        fetchers += (fetcher -> (oldVal - 1))
        fetcher ! Fetched(elem)
      }
    }
  }
}

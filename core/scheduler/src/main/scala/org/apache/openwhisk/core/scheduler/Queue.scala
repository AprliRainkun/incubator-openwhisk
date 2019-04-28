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
      trySend(None)
    case EstablishFetchStream(windows) =>
      implicit val timeout: Timeout = Timeout(5.seconds)

      val flow = Flow.fromGraph(new ActivationStage(self))
      val stream = windows.map(w => w.getWindowsSize).via(flow)
      sender ! Right(FetchStream(stream))
    case RegisterStage =>
      fetchers += (sender -> 0)
      context.watch(sender)
    case FetchRequest =>
      val oldVal = fetchers(sender)
      fetchers += (sender -> (oldVal + 1))
      trySend(Some(sender))
    case Terminated(f) =>
      fetchers -= f
  }

  private def trySend(hint: Option[ActorRef]): Unit = {
    hint map { f =>
      (f, fetchers(f))
    } orElse {
      fetchers collectFirst {
        case (fetcher, window) if window > 0 => (fetcher, window)
      }
    } foreach {
      case (fetcher, window) =>
        val send = Math.min(window, queue.size)
        (1 to send) foreach { _ =>
          val (elem, newQueue) = queue.dequeue
          fetcher ! Fetched(elem)
          queue = newQueue
        }
        fetchers += (fetcher -> (window - send))
    }
  }
}

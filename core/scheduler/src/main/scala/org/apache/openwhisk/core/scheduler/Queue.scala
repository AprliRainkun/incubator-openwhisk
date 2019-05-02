package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.stream.{ActorMaterializer, Materializer}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Queue {
  def props(name: String) = Props(new Queue(name))

  final case class Handle(queue: ActorRef, key: Long)
  final case class CancelFetch(key: Long)
  final case class RequestAtMost(key: Long, n: Int)
  final case class Response(as: List[DummyActivation])

  // fetcher will be set to Some(f) when the first RequestAtMost message is received
  private case class Registration(n: Int, fetcher: Option[ActorRef])
  private case object TKey
  private case object TTick
}

class Queue(name: String) extends Actor with Timers {
  import Queue._
  import QueueManager._

  implicit val ex: ExecutionContext = context.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  private var counter = 0L
  private var queue = immutable.Queue.empty[DummyActivation]
  private var fetchers = Map.empty[Long, Registration]

  timers.startPeriodicTimer(TKey, TTick, 5.seconds)

  override def receive: Receive = {
    case AppendActivation(act) =>
      queue = queue.enqueue(act)
      sender ! Right(Unit)
      trySend(None)
    case EstablishFetchStream(windows) =>
      val key = nextKey
      fetchers += (key -> Registration(0, None))

      val flow = ReactiveActivationFlow.create(Handle(self, key))
      val stream = windows.map(w => w.getWindowsSize).via(flow)
      sender ! Right(FetchStream(stream))
    case RequestAtMost(key, n) =>
      val old = fetchers(key).n
      fetchers += (key -> Registration(old + n, Some(sender)))
      trySend(Some(key))
    case CancelFetch(key) =>
      val Registration(n, Some(f)) = fetchers(key)
      // send en empty response to allow the ask future to complete
      if (n > 0) {
        f ! Response(List.empty)
      }
      fetchers -= key
    case TTick =>
      // prevent ask future from timing out
      fetchers collect {
        case (_, Registration(n, Some(f))) if n > 0 => f
      } foreach { f =>
        f ! Response(List.empty)
      }
  }

  private def nextKey = {
    counter += 1
    counter
  }

  private def trySend(hint: Option[Long]): Unit = {
    hint map { key =>
      (key, fetchers(key).n)
    } orElse {
      fetchers collectFirst {
        case (key, Registration(n, _)) if n > 0 => (key, n)
      }
    } foreach {
      case (key, n) =>
        val send = Math.min(n, queue.size)
        if (send > 0) {
          val es = ListBuffer.empty[DummyActivation]
          (1 to send) foreach { _ =>
            val (elem, newQueue) = queue.dequeue
            es.append(elem)
            queue = newQueue
          }
          val fO = fetchers(key).fetcher
          fetchers += (key -> Registration(n - send, fO))
          fO.head ! Response(es.toList)
        }
    }
  }
}

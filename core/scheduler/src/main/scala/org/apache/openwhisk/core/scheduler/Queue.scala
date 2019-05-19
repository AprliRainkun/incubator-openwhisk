package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, ActorRef, Props, Timers}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.DocInfo
import org.apache.openwhisk.grpc.Activation

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Queue {
  def props(action: DocInfo, schedulerConfig: SchedulerConfig)(implicit logging: Logging) =
    Props(new Queue(action, schedulerConfig))

  final case class Handle(queue: ActorRef, key: Long)
  final case class CancelFetch(key: Long)
  // n most be nonzero
  final case class RequestAtMost(key: Long, n: Int)
  final case class Response(as: List[Activation])

  // fetcher will be set to Some(f) when the first RequestAtMost message is received
  private case class Registration(n: Int, fetcher: ActorRef)
  private case object TKey
  private case object TTick
}

class Queue(action: DocInfo, schedulerConfig: SchedulerConfig)(implicit logging: Logging) extends Actor with Timers {
  import Queue._
  import QueueManager._

  implicit val ex: ExecutionContext = context.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  private var counter = 0L
  private var queue = immutable.Queue.empty[Activation]
  private var waitingList = Map.empty[Long, Registration]

  timers.startPeriodicTimer(TKey, TTick, 5.seconds)

  override def receive: Receive = {
    case AppendActivation(act) =>
      if (queue.size < schedulerConfig.maxQueueLength) {
        queue = queue.enqueue(act)
        sender ! Right(())
        logging.debug(this, s"append activation to queue, new len ${queue.size}")
        servePendingFetch()
      } else {
        sender ! Left(Overloaded(schedulerConfig.maxQueueLength))
        logging.debug(this, s"queue too long")
      }

    case EstablishFetchStream(windows) =>
      val key = nextKey
      logging.debug(this, s"reactive activation flow established, key = $key")

      val flow = ReactiveActivationFlow.create(Handle(self, key))
      val stream = windows.map(w => w.getWindowsSize).via(flow)
      sender ! Right(FetchStream(stream))
    case RequestAtMost(key, n) =>
      logging.debug(this, s"fetcher $key request $n")
      if (queue.nonEmpty) {
        dispatchAtMostNToFetcher(n, sender, key)
      } else {
        waitingList += (key -> Registration(n, sender))
      }
    case CancelFetch(key) =>
      logging.debug(this, s"received cancel from fetcher $key")
      if (waitingList.contains(key)) {
        val Registration(n, f) = waitingList(key)
        // send en empty response to allow the ask future to complete
        logging.debug(
          this,
          s"fetcher $key has requested $n elems previously, now send empty to allow the pending ask to complete")
        f ! Response(List.empty)
        waitingList -= key
      }
    case TTick =>
      // prevent ask future from timing out
      waitingList collect {
        case (_, Registration(_, f)) => f
      } foreach { f =>
        f ! Response(List.empty)
      }
  }

  private def nextKey = {
    counter += 1
    counter
  }

  private def dispatchAtMostNToFetcher(n: Int, fetcher: ActorRef, key: Long): Unit = {
    val send = Math.min(n, queue.size)
    val es = ListBuffer.empty[Activation]
    (1 to send) foreach { _ =>
      val (elem, newQueue) = queue.dequeue
      es.append(elem)
      queue = newQueue
    }
    logging.debug(this, s"dispatch ${es.size} acts to fetcher $key")
    fetcher ! Response(es.toList)
  }

  private def servePendingFetch(): Unit = {
    waitingList collectFirst {
      case (key, Registration(n, fetcher)) => (key, n, fetcher)
    } foreach {
      case (key, n, fetcher) =>
        dispatchAtMostNToFetcher(n, fetcher, key)
        waitingList -= key
    }
  }
}

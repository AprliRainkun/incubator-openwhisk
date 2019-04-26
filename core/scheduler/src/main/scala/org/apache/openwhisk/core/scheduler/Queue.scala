package org.apache.openwhisk.core.scheduler

import scala.collection.immutable
import akka.actor.{Actor, ActorRef, Props}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import akka.util.Timeout
import org.apache.openwhisk.grpc.FetchActivationResponse

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Success

object Queue {
  def props(name: String) = Props(new Queue(name))

  private case class WindowUpdate(fetcher: ActorRef, size: Int)

  private case class FetcherState(window: Int, send: SourceQueueWithComplete[DummyActivation])
  private case object FetchInit
  private case class FetchTerminate(fetcher: ActorRef, reason: Either[String, Unit])
  private case object Ack
}

class Queue(name: String) extends Actor {
  import QueueManager._
  import Queue._

  implicit val ex: ExecutionContext = context.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  private var queue = immutable.Queue.empty[DummyActivation]
  private var fetchers = Map.empty[ActorRef, FetcherState]

  override def receive: Receive = {
    case AppendActivation(act) =>
      queue = queue.enqueue(act)
      sender ! Right(Unit)
      trySendActivations(None)
    case EstablishFetchStream(windows) =>
      implicit val timeout: Timeout = Timeout(5.seconds)

      // create a source
      val fetcher = sender()
      // todo: warn when windowSize is absent (default to zero)
      windows
        .map(w => WindowUpdate(fetcher, w.getWindowsSize))
        .to(
          Sink
            .actorRefWithAck(
              self,
              FetchInit,
              Ack,
              FetchTerminate(fetcher, Right(())),
              ex => FetchTerminate(fetcher, Left(ex.toString))))
        .run()
      val (queue, actSource) = Source
      // todo: investigate buffer size
        .queue[DummyActivation](5, OverflowStrategy.backpressure)
        .toMat(Sink.asPublisher(false))(Keep.both)
        .mapMaterializedValue {
          case (q, pub) => (q, Source.fromPublisher(pub))
        }
        .run()
      fetchers += (fetcher -> FetcherState(0, queue))

      // return source to sender to complete the ask operation
      fetcher ! FetchStream(actSource)
    case FetchInit =>
    // no special handling
    case FetchTerminate(fetcher, _) =>
      // todo: add log
      fetchers -= fetcher
    case WindowUpdate(fetcher, newWindow) =>
      val FetcherState(_, send) = fetchers(fetcher)
      fetchers += (fetcher -> FetcherState(newWindow, send))
      trySendActivations(Some(fetcher))
  }

  private def trySendActivations(hint: Option[ActorRef]): Unit = {
    fetchers collectFirst {
      case (f, FetcherState(window, _)) if window > 0 => f
    } match {
      case Some(fetcher) =>
        val FetcherState(_, send) = fetchers(fetcher)
        if (queue.nonEmpty) {
          var (elem, newQueue) = queue.dequeue
          val fut = send.offer(elem) map {
            case QueueOfferResult.Enqueued =>
            case _                         =>
              // put back
              newQueue = newQueue.enqueue(elem)
          }
          // this should be immediate
          Await.result(fut, 1.second)
          queue = newQueue
        }
      case None =>
    }
//    for {
//      fetcher <- fetchers collectFirst {
//        case (f, FetcherState(window, _)) if window > 0 => f
//      }
//      elem <- queue.headOption
//      send = fetchers(fetcher).send
//      newQueue <- send.offer(elem) map {
//        case QueueOfferResult.Enqueued => queue.tail
//        case _ => queue
//      }
//    } yield newQueue
  }
}

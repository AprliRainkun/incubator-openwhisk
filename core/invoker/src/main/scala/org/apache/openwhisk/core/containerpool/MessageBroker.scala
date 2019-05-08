package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{ActorRef, FSM, Props}
import akka.pattern.pipe
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.common._
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object MessageBroker {
  def props(queueClient: QueueServiceClient, action: String, bufferLimit: Int)(implicit logging: Logging) =
    Props(new MessageBroker(queueClient, action, bufferLimit))

  // messages
  final case class NeedMessage(number: Int)
  final case class NewMessage(msg: String)

  // private messages
  private case class FlowReady(send: TickerSendEnd, messages: Source[String, NotUsed])

  // private state and events
  protected trait BrokerState
  protected case object Initing extends BrokerState
  protected case object Active extends BrokerState

  protected trait BrokerData
  protected case class Partial(send: Option[TickerSendEnd], pending: Option[(ActorRef, Int)]) extends BrokerData
  protected case class Ready(send: TickerSendEnd,
                             consumer: ActorRef,
                             buffer: Queue[NewMessage],
                             demand: Int,
                             arriving: Int)
      extends BrokerData {
    def change(buf: Queue[NewMessage], dem: Int, arr: Int) =
      Ready(send, consumer, buf, dem, arr)
  }
}

class MessageBroker(queueClient: QueueServiceClient, action: String, bufferLimit: Int)(implicit logging: Logging)
    extends FSM[MessageBroker.BrokerState, MessageBroker.BrokerData] {
  import MessageBroker._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  startWith(Initing, Partial(None, None))

  establishFetchFlow() map {
    case (send, messages) => FlowReady(send, messages)
  } pipeTo self

  when(Initing) {
    case Event(FlowReady(send, messages), data @ Partial(None, pending)) =>
      messages.runForeach { m =>
        self ! NewMessage(m)
      }
      pending match {
        case Some((consumer, demand)) =>
          val buffer = Queue.empty[NewMessage]
          val arriving = fillPipeline(send, buffer, demand, 0)
          goto(Active).using(Ready(send, consumer, buffer, demand, arriving))
        case None =>
          stay.using(data.copy(send = Some(send)))
      }
    case Event(NeedMessage(n), data @ Partial(sendO, pending)) =>
      val (consumer, demand) = pending map {
        case (con, dem) => (con, dem + n)
      } getOrElse ((sender(), n))
      sendO match {
        case Some(send) =>
          val buffer = Queue.empty[NewMessage]
          val arriving = fillPipeline(send, buffer, demand, 0)
          goto(Active).using(Ready(send, consumer, buffer, demand, arriving))
        case None =>
          stay.using(data.copy(pending = Some((consumer, demand))))
      }
  }

  when(Active) {
    case Event(r: NewMessage, d @ Ready(send, consumer, buffer, demand, arriving)) =>
      val (newBuffer, newDemand) = serveFromBuffer(consumer, buffer.enqueue(r), demand)
      val newArriving = fillPipeline(send, newBuffer, newDemand, arriving - 1)
      stay.using(d.change(newBuffer, newDemand, newArriving))
    case Event(NeedMessage(n), d @ Ready(send, consumer, buffer, demand, arriving)) =>
      // first serve from buffer
      val (newBuffer, newDemand) = serveFromBuffer(consumer, buffer, demand + n)
      // then send demand to upstream, include filling buffer
      val newArriving = fillPipeline(send, newBuffer, newDemand, arriving)
      stay.using(d.change(newBuffer, newDemand, newArriving))
  }

  private def serveFromBuffer(consumer: ActorRef, buffer: Queue[NewMessage], demand: Int): (Queue[NewMessage], Int) = {
    val provide = Math.min(demand, buffer.size)
    var newBuffer = buffer
    (0 until provide) foreach { _ =>
      val (msg, b) = buffer.dequeue
      consumer ! msg
      newBuffer = b
    }

    (newBuffer, demand - provide)
  }

  private def fillPipeline(send: TickerSendEnd, buffer: Queue[NewMessage], demand: Int, arriving: Int): Int = {
    val req = bufferLimit - buffer.size + demand - arriving
    send.send(req)
    req + arriving
  }

  private def establishFetchFlow(): Future[(TickerSendEnd, Source[String, NotUsed])] = {
    val (sendFuture, batchSizes) = Source
      .fromGraph(new ConflatedTickerStage)
      .throttle(1, 20 millis)
      .map(b => WindowAdvertisement(Message.WindowsSize(b)))
      .preMaterialize()

    val windows = Source(List(WindowAdvertisement(Message.ActionName(action))))
      .concat(batchSizes)

    val activations = queueClient
      .fetch(windows)
      .map(w => w.activation.head.body)

    sendFuture map { send =>
      (send, activations)
    }
  }
}

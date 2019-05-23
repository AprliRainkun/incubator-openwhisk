package org.apache.openwhisk.core.containerpool

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, FSM, Props, Stash}
import akka.pattern.pipe
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.database.etcd.QueueMetadataStore
import org.apache.openwhisk.core.entity.{DocInfo, QueueRegistration}
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._
import pureconfig._

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object MessageBroker {
  def props(action: DocInfo, bufferLimit: Int, queueMetadataStore: QueueMetadataStore)(implicit sys: ActorSystem,
                                                                                       logging: Logging) =
    Props(new MessageBroker(action, bufferLimit, queueMetadataStore))

  // messages
  final case class NeedMessage(number: Int)
  final case class NewMessage(msg: String)

  // private messages
  private case class FlowReady(send: TickerSendEnd, messages: Source[String, NotUsed])

  // private state and events
  protected trait BrokerState
  protected case object InitingFlow extends BrokerState
  protected case object WaitingForConsumer extends BrokerState
  protected case object Active extends BrokerState

  protected trait BrokerData
  protected case object NoData extends BrokerData
  protected case class HasFlow(send: TickerSendEnd) extends BrokerData
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

class MessageBroker(action: DocInfo, bufferLimit: Int, queueMetadataStore: QueueMetadataStore)(
  implicit sys: ActorSystem,
  logging: Logging)
    extends FSM[MessageBroker.BrokerState, MessageBroker.BrokerData]
    with Stash {
  import MessageBroker._

  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  protected val schedulerClientPool = new ClientPool[QueueServiceClient]

  startWith(InitingFlow, NoData)

  establishFetchFlow() map {
    case (send, messages) => FlowReady(send, messages)
  } pipeTo self

  when(InitingFlow) {
    case Event(FlowReady(send, messages), NoData) =>
      logging.info(this, s"activation flow established for action $action")
      messages runForeach { m =>
        self ! NewMessage(m)
      }
      goto(WaitingForConsumer).using(HasFlow(send))
    case Event(_: NeedMessage, NoData) =>
      stash()
      stay()
  }

  when(WaitingForConsumer) {
    case Event(NeedMessage(n), HasFlow(send)) =>
      val buffer = Queue.empty[NewMessage]
      val arriving = fillPipeline(send, buffer, n, 0)
      goto(Active).using(Ready(send, sender(), buffer, n, arriving))
  }

  when(Active) {
    case Event(r: NewMessage, d @ Ready(send, consumer, buffer, demand, arriving)) =>
      //println("new message arrived")
      val (newBuffer, newDemand) = serveFromBuffer(consumer, buffer.enqueue(r), demand)
      val newArriving = fillPipeline(send, newBuffer, newDemand, arriving - 1)
      //println(s"new state: buf ${newBuffer.size}, dem $newDemand arr $newArriving")
      stay.using(d.change(newBuffer, newDemand, newArriving))
    case Event(NeedMessage(n), d @ Ready(send, consumer, buffer, demand, arriving)) =>
      //println(s"demand received, $n")
      // first serve from buffer
      val (newBuffer, newDemand) = serveFromBuffer(consumer, buffer, demand + n)
      // then send demand to upstream, include filling buffer
      val newArriving = fillPipeline(send, newBuffer, newDemand, arriving)
      //println(s"new state: buf ${newBuffer.size}, dem $newDemand, arr $newArriving")
      stay.using(d.change(newBuffer, newDemand, newArriving))
  }

  onTransition {
    case InitingFlow -> WaitingForConsumer =>
      unstashAll()
    case WaitingForConsumer -> Active =>
  }

  private def serveFromBuffer(consumer: ActorRef, buffer: Queue[NewMessage], demand: Int): (Queue[NewMessage], Int) = {
    val provide = Math.min(demand, buffer.size)
    var newBuffer = buffer
    (0 until provide) foreach { _ =>
      val (msg, b) = newBuffer.dequeue
      consumer ! msg
      newBuffer = b
    }
    //println(s"$provide msgs pushed to downstream")
    (newBuffer, demand - provide)
  }

  private def fillPipeline(send: TickerSendEnd, buffer: Queue[NewMessage], demand: Int, arriving: Int): Int = {
    val req = bufferLimit - buffer.size + demand - arriving
    if (req > 0) {
      send.send(req)
      //println(s"send demand to upstream, request $req")
    } else {
      //println(s"no need to request more messages")
    }
    req + arriving
  }

  // override by unit tests
  protected def establishFetchFlow(): Future[(TickerSendEnd, Source[String, NotUsed])] = {
    queueMetadataStore.getEndPoint(action) flatMap {
      case QueueRegistration(host, port) =>
        val throttleMillis = loadConfigOrThrow[Int]("whisk.invoker.window-throttle-millis")
        val (sendFuture, sizes) = Source
          .fromGraph(new ConflatedTickerStage)
          .throttle(1, throttleMillis.millis)
          .map(b => WindowAdvertisement(Message.WindowsSize(b)))
          .preMaterialize()
        val actionId = ActionIdentifier(action.id.asString, action.rev.asString)
        val source = Source(List(WindowAdvertisement(Message.Action(actionId)))).concat(sizes)

        val client = schedulerClientPool.getClient(host, port)
        val activations = client
          .fetch(source)
          .map(_.activation.head.body)

        sendFuture map { send =>
          (send, activations)
        }
    } recoverWith {
      case t =>
        logging.error(
          this,
          s"failed to retrieve queue endpoint information from etcd for action ${action.toString}, $t")
        Future.failed(t)
    }
  }
}

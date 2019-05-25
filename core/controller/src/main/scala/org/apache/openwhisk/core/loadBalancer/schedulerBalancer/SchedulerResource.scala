package org.apache.openwhisk.core.loadBalancer.schedulerBalancer

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.Timeout
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.entity._
import spray.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

// used for implicit injection
final case class SchedulerResourceActor(actor: ActorRef)

object SchedulerResource {
  def props(config: MetadataStoreConfig)(implicit logging: Logging) = Props(new SchedulerResource(config))

  final case class CreateQueue(tid: TransactionId, actionInfo: DocInfo)
  final case class CreateQueueResult(result: Either[String, SchedulerRegistration])

  // private messages
  private case class NewScheduler(registration: SchedulerRegistration)
  private case class SchedulerOffline(instance: Int)
  private case object Ack
}

class SchedulerResource(config: MetadataStoreConfig)(implicit logging: Logging) extends Actor {
  import SchedulerResource._

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private var schedulers = Map.empty[Int, SchedulerRegistration]

  implicit val timeout: Timeout = Timeout(10 seconds)
  private val watcher = new MembershipWatcher(config)

  private val watchPrefix = config.schedulerEndpointPrefix
  logging.info(this, s"start watching scheduler membership, etcd prefix = $watchPrefix")

  watcher.watchMembership(watchPrefix) map {
    case MembershipEvent.Put(_, value) =>
      val reg = value.parseJson.convertTo[SchedulerRegistration]
      NewScheduler(reg)
    case MembershipEvent.Delete(key) =>
      val pattern = "(\\d+)$".r
      val pattern(instance) = key
      SchedulerOffline(instance.toInt)
  } map { e =>
    self ! e
  } runWith Sink.ignore

  override def receive: Receive = {
    case NewScheduler(reg) =>
      if (schedulers.contains(reg.instance)) {
        logging.warn(this, s"scheduler $reg appeared again, queue might have lost")(TransactionId.loadbalancer)
      }
      schedulers += (reg.instance -> reg)
      logging.info(this, s"new scheduler discovered, reg: $reg")(TransactionId.loadbalancer)
    case SchedulerOffline(instance) =>
      if (schedulers.contains(instance)) {
        schedulers -= instance
      }
      logging.warn(this, s"scheduler $instance offline")(TransactionId.loadbalancer)
    case CreateQueue(tid, actionInfo) =>
      implicit val transid: TransactionId = tid
      if (schedulers.isEmpty) {
        logging.error(this, s"failed to find a scheduler for action $actionInfo, no scheduler is online")
        sender ! CreateQueueResult(Left(s"no scheduler is online"))
      } else {
        val rng = new Random
        val idx = rng.nextInt(schedulers.size)
        val (_, scheduler) = schedulers.toIndexedSeq(idx)
        logging.info(this, s"reply with scheduler ${scheduler.instance}")
        sender ! CreateQueueResult(Right(scheduler))
      }
  }
}

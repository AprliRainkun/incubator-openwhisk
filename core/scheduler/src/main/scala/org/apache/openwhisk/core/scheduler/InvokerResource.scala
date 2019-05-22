package org.apache.openwhisk.core.scheduler

import akka.actor.{Actor, ActorSystem, Props, Status}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.entity._
import spray.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object InvokerResource {
  def props(config: MetadataStoreConfig)(implicit logging: Logging) = Props(new InvokerResource(config))

  final case class ReserveMemory(tid: TransactionId, memory: ByteSize, replica: Int)

  final case class Reservation(invoker: InvokerRegistration, replica: Int)
  final case class  ReserveMemoryResult(result: Either[String, Seq[Reservation]])

  // private messages
  private case class NewInvoker(registration: InvokerRegistration)
  private case class InvokerOffline(instance: Int)
}

class InvokerResource(config: MetadataStoreConfig)(implicit logging: Logging) extends Actor {
  import InvokerResource._

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private var invokers = Map.empty[Int, InvokerRegistration]
  private var invokerPools = Map.empty[Int, ByteSize]

  implicit val timeout: Timeout = Timeout(10 seconds)
  private val watcher = new MembershipWatcher(config)

  private val watchPrefix = config.invokerEndpointPrefix
  logging.info(this, s"start watching invoker membership, prefix = $watchPrefix")(TransactionId.schedulerNanny)

  watcher.watchMembership(watchPrefix) map {
    case MembershipEvent.Put(_, value) =>
      val reg = value.parseJson.convertTo[InvokerRegistration]
      NewInvoker(reg)
    case MembershipEvent.Delete(key) =>
      val pattern = "(\\d+)$".r
      val pattern(instance) = key
      InvokerOffline(instance.toInt)
  } ask self runWith Sink.ignore

  override def receive: Receive = {
    case NewInvoker(reg) =>
      if (!invokers.contains(reg.instance)) {
        invokers += (reg.instance -> reg)
        invokerPools += (reg.instance -> reg.userMemory)
      }
      sender ! Status.Success(())
      logging.info(this, s"new invoker discovered, $reg")(TransactionId.schedulerNanny)
    case InvokerOffline(instance) =>
      if (invokers.contains(instance)) {
        invokers -= instance
        invokerPools -= instance
      }
      sender ! Status.Success(())
      logging.warn(this, s"invoker offline, instance = $instance")(TransactionId.schedulerNanny)
    case ReserveMemory(tid, memory, replica) =>
      implicit val transid: TransactionId = tid
      reserve(invokerPools, memory, replica) match {
        case Some((allocations, remaining)) =>
          sender ! ReserveMemoryResult(Right(allocations))
          invokerPools = remaining
        case _ =>
          val remainingMsg = poolPrettyPrint(invokerPools)
          logging.error(this, s"failed to reserve $memory * $replica, remaining: $remainingMsg")
          sender ! ReserveMemoryResult(Left(s"failed to reserve $memory * $replica memory"))
      }
  }

  private def reserve(available: Map[Int, ByteSize],
                      requireUnit: ByteSize,
                      requireNum: Int): Option[(Seq[Reservation], Map[Int, ByteSize])] = {
    available.map {
      case (_, remain) => (remain / requireUnit).toInt
    }.sum match {
      case total if total >= requireNum =>
        var allocTo = List.empty[Int]
        var remaining = available
        (1 to requireNum) foreach { _ =>
          val (leastUse, cap) = remaining.maxBy(_._2)
          allocTo = leastUse :: allocTo
          remaining += (leastUse -> (cap - requireUnit))
        }
        val allocations = allocTo
          .groupBy(identity)
          .map {
            case (instance, ones) =>
              val invoker = invokers(instance)
              Reservation(invoker, ones.size)
          }
          .toList
        Some((allocations, remaining))
      case _ => None
    }
  }

  private def poolPrettyPrint(pool: Map[Int, ByteSize]): String = {
    pool
      .map {
        case (id, remaining) => s"($id, $remaining)"
      }
      .mkString(" ")
  }
}

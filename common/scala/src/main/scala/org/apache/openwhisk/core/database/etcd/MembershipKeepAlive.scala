package org.apache.openwhisk.core.database.etcd

import akka.actor.{ActorSystem, FSM, Props, Stash}
import akka.grpc.GrpcClientSettings
import akka.pattern.pipe
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import com.google.protobuf.ByteString
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.grpc.etcd._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object MembershipKeepAlive {
  def props(failureSeconds: Int, config: MetadataStoreConfig)(implicit logging: Logging) =
    Props(new MembershipKeepAlive(failureSeconds, config))

  final case class SetData(key: String, value: String)

  // private messages
  private case object Tick
  private case class Lease(id: Long)

  // private state and events
  protected trait MState
  protected case object Initing extends MState
  protected case object Ready extends MState

  protected trait MData
  protected case object NoData extends MData
  protected case class All(leaseId: Long, send: SourceQueueWithComplete[Unit]) extends MData
}

class MembershipKeepAlive(failureSeconds: Int, config: MetadataStoreConfig)(implicit logging: Logging)
    extends FSM[MembershipKeepAlive.MState, MembershipKeepAlive.MData]
    with Stash {
  import MembershipKeepAlive._

  implicit val sys: ActorSystem = context.system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = context.dispatcher

  private val settings = GrpcClientSettings.connectToServiceAt(config.host, config.port).withTls(false)
  private val kvClient = KVClient(settings)
  private val leaseClient = LeaseClient(settings)

  startWith(Initing, NoData)

  val req = LeaseGrantRequest(tTL = failureSeconds)
  leaseClient.leaseGrant(req) map { resp =>
    Lease(resp.iD)
  } pipeTo self

  when(Initing) {
    case Event(Lease(id), _) =>
      val (send, pingSource) = Source
        .queue[Unit](1, OverflowStrategy.dropNew)
        .map { _ =>
          LeaseKeepAliveRequest(id)
        }
        .preMaterialize()
      leaseClient.leaseKeepAlive(pingSource).runWith(Sink.ignore)
      goto(Ready).using(All(id, send))
    case Event(_: SetData, _) =>
      stash()
      stay
  }

  when(Ready) {
    case Event(SetData(key, value), All(id, _)) =>
      val req = PutRequest(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value), lease = id)
      kvClient.put(req)

      logging.info(this, s"update etcd data key = $key, value = $value")
      stay
    case Event(Tick, All(_, send)) =>
      send.offer(())
      stay
  }

  onTransition {
    case Initing -> Ready =>
      unstashAll()
      val interval = (failureSeconds seconds) / 2
      setTimer("ping", Tick, interval, repeat = true)
  }

  onTermination {
    case StopEvent(_, _, All(_, send)) => send.complete()
  }
}

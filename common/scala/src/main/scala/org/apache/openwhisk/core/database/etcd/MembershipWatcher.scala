package org.apache.openwhisk.core.database.etcd

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.google.protobuf.ByteString
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.grpc.etcd.WatchRequest.RequestUnion.CreateRequest
import org.apache.openwhisk.grpc.etcd._
import org.apache.openwhisk.grpc.mvccpb.Event.EventType

import scala.concurrent.{ExecutionContext, Promise}
import scala.language.postfixOps

sealed trait MembershipEvent
object MembershipEvent {
  final case class Put(key: String, value: String) extends MembershipEvent
  final case class Delete(key: String) extends MembershipEvent
}

class MembershipWatcher(config: MetadataStoreConfig)(implicit sys: ActorSystem,
                                                     mat: Materializer,
                                                     ex: ExecutionContext,
                                                     logging: Logging) {
  private val settings = GrpcClientSettings.connectToServiceAt(config.host, config.port).withTls(false)
  private val kvClient = KVClient(settings)
  private val watchClient = WatchClient(settings)

  def watchMembership(prefix: String): Source[MembershipEvent, NotUsed] = {
    val rangeStart = ByteString.copyFromUtf8(prefix)
    val rangeEnd = Utils.rangeEndOfPrefix(rangeStart)
    val revisionPromise = Promise[Long]
    val initReq = RangeRequest(rangeStart, rangeEnd)

    val initSnapshot = kvClient.range(initReq) map { resp =>
      revisionPromise.success(resp.header.head.revision)
      resp.kvs map { kv =>
        MembershipEvent.Put(kv.key.toStringUtf8, kv.value.toStringUtf8)
      }
    }
    val initStream = Source.fromFuture(initSnapshot).mapConcat(_.toList)
    val watchStreamFut = revisionPromise.future map { revision =>
      val watchReq = WatchRequest(CreateRequest(WatchCreateRequest(rangeStart, rangeEnd, startRevision = revision + 1)))
      watchClient.watch(Source(List(watchReq))) mapConcat { event =>
        logging.info(this, s"watch event received from etcd, count ${event.events.size}")
        event.events map { e =>
          val kv = e.kv.head
          e.`type` match {
            case EventType.PUT    => MembershipEvent.Put(kv.key.toStringUtf8, kv.value.toStringUtf8)
            case EventType.DELETE => MembershipEvent.Delete(kv.key.toStringUtf8)
            case _ =>
              logging.error(this, s"unrecognized etcd watch event received, $e")
              throw new IllegalStateException("unrecognized etcd watch event received")
          }
        } toList
      }
    }

    initStream.concat(Source.fromFutureSource(watchStreamFut))
  }
}

package org.apache.openwhisk.core.database.etcd

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.entity.{DocInfo, QueueRegistration}
import org.apache.openwhisk.grpc.etcd._
import spray.json._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object QueueMetadataStore {
  def connect(config: MetadataStoreConfig)(implicit sys: ActorSystem,
                                           mat: Materializer,
                                           ex: ExecutionContext): QueueMetadataStore = {
    val settings = GrpcClientSettings.connectToServiceAt(config.host, config.port).withTls(false)
    new QueueMetadataStore(config, KVClient(settings))
  }
}

class QueueMetadataStore(config: MetadataStoreConfig, kvClient: KVClient) {
  private val endpointCache = TrieMap.empty[DocInfo, QueueRegistration]

  def txnMarkCreating(action: DocInfo): Future[Unit] = {
    val _ = config.queueMarkerKeyTemplate.format(action.id.asString, action.rev.asString)
    //TODO: implement check
    Future.successful(())
  }

  def txnWriteEndpoint(action: DocInfo, registration: QueueRegistration)(
    implicit ctx: ExecutionContext): Future[Unit] = {
    val key = config.queueEndpointKeyTemplate.format(action.id.asString, action.rev.asString)
    val value = registration.toJson.compactPrint
    val req = PutRequest(key = ByteString.copyFromUtf8(key), value = ByteString.copyFromUtf8(value))

    kvClient.put(req).map(_ => ())
  }

  def getEndPoint(action: DocInfo, refreshCache: Boolean = false)(
    implicit ex: ExecutionContext): Future[QueueRegistration] = {

    def readDbAndUpdateCache() = readDatabase(action) andThen {
      case Success(reg) => endpointCache.put(action, reg)
      case _            =>
    }
    if (refreshCache) {
      readDbAndUpdateCache()
    } else {
      endpointCache.get(action) match {
        case Some(reg) => Future successful reg
        case None =>
          readDbAndUpdateCache()
      }
    }
  }

  private def readDatabase(action: DocInfo)(implicit ex: ExecutionContext): Future[QueueRegistration] = {
    val key = config.queueEndpointKeyTemplate.format(action.id.asString, action.rev.asString)
    val req = RangeRequest(key = ByteString.copyFromUtf8(key))
    kvClient.range(req) flatMap { resp =>
      if (resp.count < 1) {
        Future.failed(new NoSuchElementException(s"key $key doesn't exist in etcd"))
      } else {
        val raw = resp.kvs.head.value.toStringUtf8
        val reg = raw.parseJson.convertTo[QueueRegistration]
        Future.successful(reg)
      }
    }
  }
}

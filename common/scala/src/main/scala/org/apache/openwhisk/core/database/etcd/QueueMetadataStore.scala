package org.apache.openwhisk.core.database.etcd

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.entity.DocInfo
import org.apache.openwhisk.grpc.etcd._

import scala.concurrent.{ExecutionContext, Future}

final case class QueueMetadataStoreConfig(markerKeyTemplate: String,
                                          endpointKeyTemplate: String,
                                          host: String,
                                          port: Int)

object QueueMetadataStore {
  def connect(config: QueueMetadataStoreConfig)(implicit sys: ActorSystem,
                                                mat: Materializer,
                                                ex: ExecutionContext): QueueMetadataStore = {
    val settings = GrpcClientSettings.connectToServiceAt(config.host, config.port).withTls(false)
    new QueueMetadataStore(config, KVClient(settings))
  }
}

class QueueMetadataStore(config: QueueMetadataStoreConfig, kvClient: KVClient) {
  def txnMarkCreating(action: DocInfo): Future[Unit] = {
    val _ = config.markerKeyTemplate.format(action.id.asString, action.rev.asString)
    //TODO: implement check
    Future.successful(())
  }

  def txnWriteEndpoint(action: DocInfo, endpoint: String)(implicit ctx: ExecutionContext): Future[Unit] = {
    val key = config.endpointKeyTemplate.format(action.id.asString, action.rev.asString)
    val req = PutRequest(key = ByteString.copyFromUtf8(key), value = ByteString.copyFromUtf8(endpoint))

    kvClient.put(req).map(_ => ())
  }

  def getEndPoint(action: DocInfo)(implicit ex: ExecutionContext): Future[(String, Int)] = {
    val key = config.endpointKeyTemplate.format(action.id.asString, action.rev.asString)
    val req = RangeRequest(key = ByteString.copyFromUtf8(key))
    kvClient.range(req) flatMap { resp =>
      if (resp.count < 1) {
        Future.failed(new NoSuchElementException(s"queue endpoint for action ${action.toString} doesn't exist in etcd"))
      } else {
        val endpoint = resp.kvs.head.value.toStringUtf8
        endpoint.split(":") match {
          case Array(host, port) => Future.successful((host, port.toInt))
          case _                 => Future.failed(new IllegalStateException(s"endpoint $endpoint isn't in form host:port"))
        }
      }
    }
  }
}

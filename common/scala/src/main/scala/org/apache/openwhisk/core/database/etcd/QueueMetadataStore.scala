package org.apache.openwhisk.core.database.etcd

import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.google.protobuf.ByteString
import org.apache.openwhisk.grpc.etcd._

import scala.concurrent.{ExecutionContext, Future}

final case class QueueMetadataStoreConfig(markerKeyTemplate: String, endpointKeyTemplate: String)

object QueueMetadataStore {
  def connect(config: QueueMetadataStoreConfig, settings: GrpcClientSettings)(implicit mat: Materializer,
                                                                              ex: ExecutionContext) =
    new QueueMetadataStore(config, KVClient(settings))
}

class QueueMetadataStore(config: QueueMetadataStoreConfig, kvClient: KVClient) {
  def txnMarkCreating(actionName: String): Future[Unit] = {
    val _ = config.markerKeyTemplate.format(actionName)
    //TODO: implement check
    Future.successful(())
  }

  def txnWriteEndpoint(actionName: String, endpoint: String)(implicit ctx: ExecutionContext): Future[Unit] = {
    val key = config.endpointKeyTemplate.format(actionName)
    val req = PutRequest(key = ByteString.copyFromUtf8(key), value = ByteString.copyFromUtf8(endpoint))

    kvClient.put(req).map(_ => ())
  }
}

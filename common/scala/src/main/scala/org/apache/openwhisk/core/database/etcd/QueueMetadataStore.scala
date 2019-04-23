package org.apache.openwhisk.core.database.etcd

import akka.grpc.GrpcClientSettings
import akka.stream.Materializer
import com.google.protobuf.ByteString
import org.apache.openwhisk.grpc.etcd._

import scala.concurrent.{ExecutionContext, Future}

object QueueMetadataStore {
  def connect(settings: GrpcClientSettings)(implicit mat: Materializer, ex: ExecutionContext) =
    new QueueMetadataStore(KVClient(settings))
}

class QueueMetadataStore(kvClient: KVClient) {
  def txnMarkCreating(actionName: String): Future[Unit] = {
    val _ = s"queue/$actionName/creating"
    //TODO: implement check
    Future.successful(())
  }

  def txnWriteEndpoint(actionName: String, endpoint: String)(implicit ctx: ExecutionContext): Future[Unit] = {
    val key = s"queue/$actionName/endpoint"
    val req = PutRequest(key = ByteString.copyFromUtf8(key), value = ByteString.copyFromUtf8(endpoint))

    kvClient.put(req).map(_ => ())
  }
}

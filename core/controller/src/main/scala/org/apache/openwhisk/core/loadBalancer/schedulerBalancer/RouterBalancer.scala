package org.apache.openwhisk.core.loadBalancer.schedulerBalancer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.controller.RejectRequest
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.loadBalancer._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.grpc.{TransactionId => RpcTid, Activation => RpcActivation, _}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import spray.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}

class RouterBalancer(config: WhiskConfig,
                     feedFactory: FeedFactory,
                     controllerInstance: ControllerInstanceId,
                     implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  mat: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  private val metadataStoreConfig = loadConfigOrThrow[MetadataStoreConfig](ConfigKeys.metadataStore)
  private val queueMetadataStore = QueueMetadataStore.connect(metadataStoreConfig)
  private val schedulerClientPool = new ClientPool[QueueServiceClient]

  private val placeholder = InvokerInstanceId(0, None, None, ByteSize.fromString("256GB"))

  logging.info(this, "start router balancer")

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    // first check the existence of the action queue, get scheduler endpoint
    // then setup up activation
    // finally push the message to scheduler
    val actionInfo = action.docinfo

    for {
      QueueRegistration(host, port) <- queueMetadataStore.getEndPoint(actionInfo)
      client = schedulerClientPool.getClient(host, port)
      resultFuture = setupActivation(msg, action, placeholder)
      rpcTid = RpcTid(transid.toJson.compactPrint)
      actionId = ActionIdentifier(actionInfo.id.asString, actionInfo.rev.asString)
      req = RpcActivation(Some(rpcTid), Some(actionId), msg.toJson.compactPrint)
      _ <- client.put(req) transform {
        case Success(resp) =>
          val status = resp.status.head
          status.statusCode match {
            case 200 => Success(())
            case 429 => Failure(RejectRequest(StatusCodes.TooManyRequests, status.message))
            case _   => Failure(new IllegalStateException(status.message))
          }
        case f @ _ => f
      }
    } yield resultFuture
  }

  override protected val invokerPool: ActorRef = actorSystem.actorOf(Props.empty)

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(IndexedSeq.empty[InvokerHealth])

  override def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {
    // does nothing
  }

  override def clusterSize: Int = 1
}

object RouterBalancer extends LoadBalancerProvider {
  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    mat: ActorMaterializer): LoadBalancer =
    new RouterBalancer(whiskConfig, createFeedFactory(whiskConfig, instance), instance)

  override def requiredProperties: Map[String, String] = WhiskConfig.kafkaHosts
}

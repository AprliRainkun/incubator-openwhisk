package org.apache.openwhisk.core.invoker

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.database.etcd.{QueueMetadataStore, QueueMetadataStoreConfig}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

object InvokerPull extends InvokerProvider {
  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerPull(config, instance, producer, poolConfig, limitsConfig)
}

class InvokerPull(config: WhiskConfig,
                  instance: InvokerInstanceId,
                  producer: MessageProducer,
                  poolConfig: ContainerPoolConfig,
                  limitsConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging)
    extends InvokerCore {

  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ex: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config
  implicit val invokerInstanceId: InvokerInstanceId = instance

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  /**
   * Factory used by the ContainerProxy to physically create a new container.
   *
   * Create and initialize the container factory before kicking off any other
   * task or actor because further operation does not make sense if something
   * goes wrong here. Initialization will throw an exception upon failure.
   */
  private val containerFactory =
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(
        actorSystem,
        logging,
        config,
        instance,
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")) ++ logsProvider.containerParameters)
  containerFactory.init()
  sys.addShutdownHook(containerFactory.cleanup())

  /** Initialize needed databases */
  private val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, mat, logging)
  private val queueMetadataStoreConfig = loadConfigOrThrow[QueueMetadataStoreConfig](ConfigKeys.queueMetadataStore)
  private val queueMetadataStore = QueueMetadataStore.connect(queueMetadataStoreConfig)

  // pool manager
  private val poolManager = actorSystem.actorOf(
    PoolManager
      .props(containerFactory.createContainer, entityStore, activationStore, poolConfig, queueMetadataStore, producer))
  // setup rpc endpoint
  private val rpcImpl = new RpcEndpoint(poolManager)
  private val port = loadConfigOrThrow[Int]("whisk.invoker.grpc.port")

  private val serveFut = new RpcServer(rpcImpl).run("0.0.0.0", port)
  Await.result(serveFut, 3 seconds)
}

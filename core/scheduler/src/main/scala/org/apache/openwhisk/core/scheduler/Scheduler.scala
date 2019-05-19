package org.apache.openwhisk.core.scheduler

import akka.actor.ActorSystem
import akka.event
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common._
import org.apache.openwhisk.core._
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.utils._
import pureconfig._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.Try

case class CmdLineArgs(id: Int)

object CmdLineArgs {
  def parse(ls: List[String]): Option[CmdLineArgs] = {
    ls match {
      case "--id" :: id :: _ if Try(id.toInt).isSuccess =>
        Some(CmdLineArgs(id.toInt))
      case _ => None
    }
  }
}

object Scheduler {
  def main(args: Array[String]): Unit = {
    ConfigMXBean.register()
    implicit val ef: ExecutionContext = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
    implicit val actorSystem: ActorSystem = ActorSystem("scheduler-actor-system", defaultExecutionContext = Some(ef))
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val logger: Logging = new AkkaLogging(event.Logging.getLogger(actorSystem, this))

    def abort(msg: String) = {
      logger.error(this, msg)(TransactionId.schedulerStarting)
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30 seconds)
      sys.exit(1)
    }

    val metadataStoreConfig = loadConfigOrThrow[MetadataStoreConfig](ConfigKeys.metadataStore)
    val schedulerConfig = loadConfigOrThrow[SchedulerConfig](ConfigKeys.scheduler)
    val cmdLineArgs = CmdLineArgs.parse(args.toList) match {
      case Some(cla) => cla
      case None      => abort("Failed to parse cmd args")
    }

    // create database connection
    val entityStore = WhiskEntityStore.datastore()
    val queueMetadataStore = QueueMetadataStore.connect(metadataStoreConfig)

    // membership keepalive
    val failureSeconds = loadConfigOrThrow[Int]("whisk.metadata-store.failure-seconds")
    val membership = actorSystem.actorOf(MembershipKeepAlive.props(failureSeconds, metadataStoreConfig))

    // start queue manager
    val queueManager = actorSystem.actorOf(QueueManager.props(schedulerConfig), "queue-manager")

    // start invoker monitor
    val invokerResource = actorSystem.actorOf(InvokerResource.props(metadataStoreConfig))

    // start rpc service
    val rpcService = new RpcEndpoint(queueManager, invokerResource, queueMetadataStore, entityStore, schedulerConfig)
    val serveFut = new QueueServiceServer(rpcService)
      .run("0.0.0.0", schedulerConfig.port)
    Await.result(serveFut, 5 seconds)

    // write membership information
    val registration = SchedulerRegistration(cmdLineArgs.id, schedulerConfig.host, schedulerConfig.port)
    val schedulerKey = metadataStoreConfig.schedulerEndpointKeyTemplate.format(registration.instance)
    val schedulerValue = registration.toJson.compactPrint
    membership ! MembershipKeepAlive.SetData(schedulerKey, schedulerValue)

  }
}

package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import com.google.protobuf.ByteString
import org.apache.openwhisk.common.{AkkaLogging, Logging}
import org.apache.openwhisk.core.database.etcd.Utils.rangeEndOfPrefix
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.database.memory._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler._
import org.apache.openwhisk.grpc.QueueServiceClient
import org.apache.openwhisk.grpc.etcd._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.reflect.classTag

@RunWith(classOf[JUnitRunner])
abstract class TestBase(sysName: String)
    extends TestKit(ActorSystem(sysName))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  implicit val sys: ActorSystem = system
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val mat: ActorMaterializer = ActorMaterializer()

  def etcdHost: String
  def etcdPort: Int

  val entityPrefix = "__unit_test_temp_objects/"

  val etcdClientSettings: GrpcClientSettings = GrpcClientSettings.connectToServiceAt(etcdHost, etcdPort).withTls(false)

  override def afterAll(): Unit = {
    val prefix = entityPrefix.toByteString
    val req = DeleteRangeRequest(key = prefix, rangeEnd = rangeEndOfPrefix(prefix))
    val fut = kvClient.deleteRange(req)
    Await.result(fut, 5.seconds)

    system.terminate()
    Await.result(system.whenTerminated, 10.seconds)
  }

  def kvClient = KVClient(etcdClientSettings)

  implicit class StringExt(str: String) {
    def toByteString: ByteString = ByteString.copyFromUtf8(str)
  }
}

trait LocalScheduler { this: TestBase =>
  def schedulerPort: Int

  private val local = "127.0.0.1"

  val metadataStoreConfig = MetadataStoreConfig(
    entityPrefix + "queue/%s#%s/marker",
    entityPrefix + "queue/%s#%s/endpoint",
    entityPrefix + "membership/scheduler%d",
    entityPrefix + "membership/invoker%d",
    etcdHost,
    etcdPort)

  val queueMetadataStore: QueueMetadataStore = QueueMetadataStore.connect(metadataStoreConfig)

  def schedulerClient: QueueServiceClient = {
    val config = GrpcClientSettings.connectToServiceAt(local, schedulerPort).withTls(false)
    QueueServiceClient(config)
  }

  override def beforeAll(): Unit = {
    implicit val logging: Logging = new AkkaLogging(akka.event.Logging.getLogger(sys, this))

    val schedulerConfig: SchedulerConfig = SchedulerConfig(local, schedulerPort, 0, 10000)

    val manager = system.actorOf(QueueManager.props(schedulerConfig))
    val invokerResourceStub = TestProbe()
    // actually not used
    val memoryStore =
      MemoryArtifactStoreProvider.makeArtifactStore[WhiskEntity](MemoryAttachmentStoreProvider.makeStore())(
        classTag[WhiskEntity],
        WhiskEntityJsonFormat,
        WhiskDocumentReader,
        system,
        logging,
        mat)
    val srv = new QueueServiceServer(
      new RpcEndpoint(manager, invokerResourceStub.ref, queueMetadataStore, memoryStore, schedulerConfig))
      .run(local, schedulerPort)
    Await.result(srv, 5.seconds)
  }
}

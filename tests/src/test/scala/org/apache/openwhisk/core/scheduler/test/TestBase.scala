package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.database.etcd.QueueMetadataStoreConfig
import org.apache.openwhisk.core.database.etcd.Utils.rangeEndOfPrefix
import org.apache.openwhisk.core.scheduler._
import org.apache.openwhisk.grpc.QueueServiceClient
import org.apache.openwhisk.grpc.etcd._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

@RunWith(classOf[JUnitRunner])
abstract class TestBase(sysName: String)
    extends TestKit(ActorSystem(sysName))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

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

  val storeConfig = QueueMetadataStoreConfig(entityPrefix + "queue/%s/marker", entityPrefix + "queue/%s/endpoint")

  private val local = "127.0.0.1"

  def schedulerClient: QueueServiceClient = {
    val config = GrpcClientSettings.connectToServiceAt(local, schedulerPort).withTls(false)
    QueueServiceClient(config)
  }

  override def beforeAll(): Unit = {
    implicit val etcdSettings: GrpcClientSettings = etcdClientSettings
    implicit val schedulerConfig: SchedulerConfig = SchedulerConfig(local, storeConfig)

    val manager = system.actorOf(QueueManager.props(etcdSettings, schedulerConfig))

    val srv = new QueueServiceServer(new RpcEndpoint(manager)).run(local, schedulerPort)
    Await.result(srv, 5.seconds)
  }
}

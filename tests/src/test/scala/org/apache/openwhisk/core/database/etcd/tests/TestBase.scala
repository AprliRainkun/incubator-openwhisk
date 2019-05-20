package org.apache.openwhisk.core.database.etcd.tests

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import org.apache.openwhisk.common.{AkkaLogging, Logging}
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.grpc.etcd._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
abstract class TestBase(name: String)
    extends TestKit(ActorSystem(name))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val logging: Logging = new AkkaLogging(akka.event.Logging.getLogger(system, this))

  def etcdHost: String
  def etcdPort: Int

  private val entityPrefix = "__unit_test_temp_objects/"
  private val settings = GrpcClientSettings.connectToServiceAt(etcdHost, etcdPort).withTls(false)

  val metadataStoreConfig = MetadataStoreConfig(
    entityPrefix + "queue/%s#%s/marker",
    entityPrefix + "queue/%s#%s/endpoint",
    entityPrefix + "membership/scheduler/%d",
    entityPrefix + "membership/invoker/%d",
    etcdHost,
    etcdPort)

  val kvClient = KVClient(settings)

  override def afterAll(): Unit = {
    val prefix = ByteString.copyFromUtf8(entityPrefix)
    val req = DeleteRangeRequest(prefix, Utils.rangeEndOfPrefix(prefix))
    val fut = kvClient.deleteRange(req) flatMap { _ =>
      system.terminate()
      system.whenTerminated
    }
    Await.result(fut, 10 seconds)
  }

  implicit class StringExt(str: String) {
    def toByteString: ByteString = ByteString.copyFromUtf8(str)
  }
}

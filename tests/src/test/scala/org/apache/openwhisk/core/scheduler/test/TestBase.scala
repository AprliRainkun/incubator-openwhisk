package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.database.etcd.Utils.rangeEndOfPrefix
import org.apache.openwhisk.grpc.etcd.{DeleteRangeRequest, KVClient}
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

  protected val entityPrefix = "__unit_test_temp_objects/"
  protected def etcdClientSettings: GrpcClientSettings

  override def afterAll(): Unit = {
    val prefix = entityPrefix.toByteString
    val req = DeleteRangeRequest(key = prefix, rangeEnd = rangeEndOfPrefix(prefix))
    val fut = kvClient.deleteRange(req)
    Await.result(fut, 5.seconds)

    system.terminate()
    Await.result(system.whenTerminated, 10.seconds)
  }

  protected def kvClient = KVClient(etcdClientSettings)

  implicit protected class StringExt(str: String) {
    def toByteString: ByteString = ByteString.copyFromUtf8(str)
  }

}

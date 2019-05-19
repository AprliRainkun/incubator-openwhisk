package org.apache.openwhisk.core.database.etcd.tests

import akka.stream.testkit.scaladsl.TestSink
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.database.etcd._
import spray.json._

import scala.concurrent.duration._
import scala.language.postfixOps

class MembershipCombinedTests extends TestBase("MembershipCombinedTests") {
  lazy val etcdHost = "127.0.0.1"
  lazy val etcdPort = 2379

  val failureSeconds = 2

  "Membership service" should {
    "register invoker and then show it via watcher" in {
      val invokerId = 0
      val invokerHost = "172.17.0.1"
      val invokerPort = 12001
      val userMemory = ByteSize.fromString("4096MB")

      val registration = InvokerRegistration(invokerId, invokerHost, invokerPort, userMemory)
      val invokerKey = metadataStoreConfig.invokerEndpointKeyTemplate.format(registration.instance)
      val invokerValue = registration.toJson.compactPrint

      val keepAlive = system.actorOf(MembershipKeepAlive.props(failureSeconds, metadataStoreConfig))
      val watcher = new MembershipWatcher(metadataStoreConfig)
      val probe = watcher.watchMembership(metadataStoreConfig.invokerEndpointPrefix).runWith(TestSink.probe)

      keepAlive ! MembershipKeepAlive.SetData(invokerKey, invokerValue)
      probe.request(2)

      val discovered = probe.expectNext(2 second)
      discovered shouldBe an[MembershipEvent.Put]
      val asserted = discovered.asInstanceOf[MembershipEvent.Put]

      asserted.key should be(invokerKey)

      val discoveredInvoker = asserted.value.parseJson.convertTo[InvokerRegistration]
      discoveredInvoker.instance should be(invokerId)
      discoveredInvoker.host should be(invokerHost)
      discoveredInvoker.port should be(invokerPort)
      discoveredInvoker.userMemory should be(userMemory)

      probe.expectNoMessage((failureSeconds * 3) seconds)

      succeed
    }
  }
}

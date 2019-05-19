package org.apache.openwhisk.core.database.etcd

final case class MetadataStoreConfig(queueMarkerKeyTemplate: String,
                                     queueEndpointKeyTemplate: String,
                                     schedulerEndpointKeyTemplate: String,
                                     invokerEndpointKeyTemplate: String,
                                     host: String,
                                     port: Int) {
  def schedulerEndpointPrefix: String = schedulerEndpointKeyTemplate.replaceAll("[^/]+$", "")

  def invokerEndpointPrefix: String = invokerEndpointKeyTemplate.replaceAll("[^/]+$", "")
}

package org.apache.openwhisk.core.database.etcd

final case class MetadataStoreConfig(queueMarkerKeyTemplate: String,
                                     queueEndpointKeyTemplate: String,
                                     schedulerEndpointKeyTemplate: String,
                                     invokerEndpointKeyTemplate: String,
                                     host: String,
                                     port: Int)

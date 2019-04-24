package org.apache.openwhisk.core.database.etcd

import com.google.protobuf.ByteString

object Utils {
  def rangeEndOfPrefix(bytes: ByteString): ByteString = {
    var end = new Array[Byte](bytes.size)
    bytes.copyTo(end, 0)
    for (i <- (end.length - 1) to 0 by -1) {
      if (end(i) <= 0xff) {
        end(i) = (end(i) + 1).toByte
        end = end.slice(0, i + 1)
        return ByteString.copyFrom(end)
      }
    }

    ByteString.copyFrom(Array.fill[Byte](1)(0))
  }
}

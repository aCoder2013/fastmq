package com.song.fastmq.storage.storage.config

import com.google.common.base.Charsets
import org.apache.bookkeeper.client.BookKeeper

/**
 * Created by song on 2017/11/5.
 */
class BookKeeperConfig {

    var ensSize = 3

    var writeQuorumSize = 2

    var ackQuorumSize = 2

    var digestType: BookKeeper.DigestType = BookKeeper.DigestType.MAC

    var password = "".toByteArray(Charsets.UTF_8)
}

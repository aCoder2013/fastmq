package com.song.fastmq.storage.storage.metadata

import java.util.*

/**
 *
 * Created by song on 2017/11/5.
 */
class Log(var name: String = "") {

    var segments: List<LogSegment> = Collections.emptyList()
}

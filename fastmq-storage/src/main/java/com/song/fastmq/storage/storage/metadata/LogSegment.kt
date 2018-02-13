package com.song.fastmq.storage.storage.metadata

/**
 * Created by song on 2017/11/5.
 */
data class LogSegment(var ledgerId: Long = 0, var entries: Long = 0, var size: Long = 0, var timestamp: Long = 0)

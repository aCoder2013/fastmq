package io.openmessaging.fastmq.exception

/**
 * @author song
 */
enum class ErrorCode(val code: Int) {

    CONNECTION_LOSS(1),

    WRONG_MESSAGE_FORMAT(2),

    SYSTEM_ERROR(5000)
}
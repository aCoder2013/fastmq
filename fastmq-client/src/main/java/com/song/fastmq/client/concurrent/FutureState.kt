package com.song.fastmq.client.concurrent

/**
 * @author song
 */
enum class FutureState(val value: Int) {

    /**
     * the task is doing
     **/
    DOING(0),
    /**
     * the task is done
     **/
    DONE(1),
    /**
     * ths task is cancelled
     **/
    CANCELLED(2);

    fun isCancelledState(): Boolean = this == CANCELLED

    fun isDoneState(): Boolean = this == DONE

    fun isDoingState(): Boolean = this == DOING

}
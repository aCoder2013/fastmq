package com.song.fastmq.broker.core

import com.song.fastmq.broker.BrokerService
import com.song.fastmq.common.logging.LoggerFactory
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.net.proto.BrokerRemotingApi
import com.song.fastmq.net.proto.BrokerRemotingServiceGrpc
import io.grpc.Status
import io.grpc.stub.StreamObserver
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class BrokerRemotingService(private val brokerService: BrokerService) : BrokerRemotingServiceGrpc.BrokerRemotingServiceImplBase() {

    private val producers = ConcurrentHashMap<String, CompletableFuture<Producer>>()

    override fun handleProducerRequest(request: BrokerRemotingApi.ProducerRequest, responseObserver: StreamObserver<BrokerRemotingApi.ProducerSuccessReply>) {
        val producerName: String = if (request.producerName.isNullOrBlank()) {
            UUID.randomUUID().toString().replace("-", "")
        } else {
            request.producerName
        }

        val topic = request.topic
        val producerId = request.producerId
        val producerFuture = CompletableFuture<Producer>()
        val existingProducerFuture = producers.putIfAbsent(producerName, producerFuture)
        existingProducerFuture?.run {
            if (existingProducerFuture.isDone && !existingProducerFuture.isCompletedExceptionally) {
                logger.info("Producer with the same id is already created: {}", producerId)
                responseObserver.onNext(BrokerRemotingApi.ProducerSuccessReply.newBuilder().setProducerName(producerName).build())
                responseObserver.onCompleted()
                return
            } else {
                var status = Status.UNKNOWN.withDescription("Unknown error while creating producer[$producerName]")
                if (!existingProducerFuture.isDone) {
                    status = Status.INTERNAL.withDescription("Producer[$producerName] is not ready yet.")
                } else {
                    try {
                        existingProducerFuture.getNow(null)
                    } catch (e: Exception) {
                        logger.error("Got exception while got existing producer[$producerName]", e)
                        status = Status.UNKNOWN.withDescription("[$producerName] Fail to create producer.").withCause(e)
                    }
                }
                logger.warn("[$producerId] Producer with same id is already connected")
                responseObserver.onError(status.asRuntimeException())
                return
            }
        }?.run {
            logger.info("[{}] Try to create producer with name [{}]", topic, producerName)
            brokerService.getTopic(topic).subscribe(object : OnCompletedObserver<Topic>() {
                override fun onError(e: Throwable) {
                    producers[producerName]?.completeExceptionally(e)
                    logger.error("Open topic failed_" + e.message, e)
                    responseObserver.onError(Status.UNKNOWN.withDescription("Create Topic failed for producer $producerName").withCause(e).asRuntimeException())
                }

                override fun onNext(t: Topic) {
                    val producer = Producer(t, producerName, producerId)
                    producers[producerName]?.run {
                        complete(producer)
                        t.addProducer(producer)
                        responseObserver.onNext(BrokerRemotingApi.ProducerSuccessReply.newBuilder().setProducerName(producerName).build())
                        responseObserver.onCompleted()
                    }
                }
            })
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(BrokerRemotingService::class.java)
    }

}

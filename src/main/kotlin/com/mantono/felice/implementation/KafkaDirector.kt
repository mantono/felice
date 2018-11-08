package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Infinite
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.RetryPolicy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.Semaphore

private val log = KotlinLogging.logger("kafka-director")

class KafkaDirector<K, V>(
	private val consumer: KafkaConsumer<K, V>,
	private val retryPolicy: RetryPolicy,
	private val work: Channel<Message<K, V>> = Channel(1000),
	private val results: Channel<MessageResult<K, V>> = Channel(1000)
):
	Director<K, V>,
	ReceiveChannel<Message<K, V>> by work,
	SendChannel<MessageResult<K, V>> by results {

	constructor(consumer: KafkaConsumer<K, V>, retryPolicy: RetryPolicy = Infinite, bufferSize: Int = 1_000):
			this(consumer, retryPolicy, Channel(bufferSize), Channel(bufferSize))

	private val allowStart = Semaphore(1)
	override fun start(scope: CoroutineScope): Boolean {
		return allowStart.tryAcquire().onTrue {
			scope.launch {
				pollWork(scope)
			}
			scope.launch {
				processResult(scope)
			}
		}
	}

	private tailrec suspend fun pollWork(scope: CoroutineScope) {
		if(!scope.isActive) {
			consumer.close()
			work.close()
			results.close()
			return
		}

		consumer.poll(Duration.ofSeconds(3))
			.map { Message(it) }
			.onEach { log.debug { "Received message: $it" } }
			.forEach {
				work.send(it)
				log.debug { "Sent message $it" }
			}

		pollWork(scope)
	}

	private tailrec suspend fun processResult(scope: CoroutineScope) {
		if(!scope.isActive) {
			return
		}

		val result: MessageResult<K, V> = results.receive()
		val offset = OffsetAndMetadata(result.nextOffset)
		logResult(result)
		if(!retryPolicy.shouldRetry(result)) {
			log.debug { "Offset (${offset.offset()}) committed for ${result.message.topicPartition}" }
			consumer.commitSync(mapOf(result.message.topicPartition to offset))
		} else {
			log.debug { "Retrying message for ${result.message.topicPartition} at ${offset.offset()}" }
			work.send(result.message)
		}
		processResult(scope)
	}

	private fun logResult(result: MessageResult<K, V>) {
		val tp: TopicPartition = result.message.topicPartition
		when(val res = result.result) {
			ConsumerResult.Success ->
				log.debug { "Success: $tp" }
			is ConsumerResult.PermanentFailure ->
				log.error { "Got permanent failure when processing job: ${res.message} for $tp" }
			is ConsumerResult.TransitoryFailure ->
				log.error { "Got transitory failure when processing job: ${res.message} for $tp" }
		}
	}
}

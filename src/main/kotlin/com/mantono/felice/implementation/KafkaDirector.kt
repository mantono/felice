package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.worker.Connection
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
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
	private val consumer: Connection<K, V>,
	private val worker: Worker<K, V>,
	private val results: Channel<MessageResult> = Channel(1000)
):
	Director,
	SendChannel<MessageResult> by results {

	private val allowStart = Semaphore(1)

	override fun start(scope: CoroutineScope): Boolean {
		return allowStart.tryAcquire().onTrue {
			scope.launch {
				val actors = ActorSwarm(worker, results, scope)
				pollWork(scope, actors)
			}
			scope.launch {
				processResult(scope)
			}
		}
	}

	private tailrec suspend fun pollWork(scope: CoroutineScope, actors: ActorSwarm<K, V>) {
		if(!scope.isActive) {
			consumer.close()
			results.close()
			return
		}

		consumer.poll(Duration.ofSeconds(1))
			.onEach { log.debug { "Received message: $it" } }
			.forEach {
				actors.send(it)
				log.debug { "Sent message $it" }
			}

		pollWork(scope, actors)
	}

	private tailrec suspend fun processResult(scope: CoroutineScope) {
		if(!scope.isActive) {
			return
		}

		val result: MessageResult = results.receive()
		logResult(result)
		consumer.commit(result)
		processResult(scope)
	}

	private fun logResult(result: MessageResult) {
		val tp: TopicPartition = result.topicPartition
		val msg = "${result.result} [$tp, offset ${result.offset}]"
		when(result.result) {
			ConsumerResult.Success ->
				log.debug { msg }
			is ConsumerResult.PermanentFailure ->
				log.error { msg }
			is ConsumerResult.TransitoryFailure ->
				log.error { msg }
		}
	}
}

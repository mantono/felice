@file:Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")

package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.RetryPolicy
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.PartitionInfo
import java.lang.IllegalArgumentException
import java.time.Duration
import java.util.concurrent.Semaphore
import kotlin.coroutines.CoroutineContext

private val log = KotlinLogging.logger("felice-engine")

fun <K, V> Worker<K, V>.start(): CoroutineContext = execute(this)

class WorkDistributor<K, V>(
	private val worker: Worker<K, V>,
	private val consumer: KafkaConsumer<K, V>,
	private val work: Channel<Message<K, V>> = Channel(200),
	private val results: Channel<MessageResult<K, V>> = Channel(200)
): ReceiveChannel<MessageResult<K, V>> by results {
	private val allowStart = Semaphore(1)

	fun start(scope: CoroutineScope = GlobalScope): Boolean {
		return allowStart.tryAcquire().onTrue {
			scope.launch { work(scope) }
		}
	}

	private tailrec fun work(scope: CoroutineScope) {
		if(!scope.isActive) {
			log.info { "Cancelling current job" }
			consumer.close(Duration.ofSeconds(30))
			results.close()
			work.close()
			return
		}

		log.debug { "Polling..." }
		consumer.poll(Duration.ofSeconds(10))
			.map { Message(it) }
			.onEach { log.debug { "Received message: $it" } }
			.forEach { launchConsumer(scope, worker, results, it) }

		work(scope)
	}
}

class ResultHandler<K, V>(
	private val results: ReceiveChannel<MessageResult<K, V>>,
	private val work: SendChannel<Message<K, V>>,
	private val consumer: KafkaConsumer<K, V>
) {
	private val allowStart = Semaphore(1)

	fun start(scope: CoroutineScope): Boolean {
		return allowStart.tryAcquire().onTrue {
			scope.launch { work(scope) }
		}
	}

	private tailrec suspend fun work(scope: CoroutineScope) {
		if(!scope.isActive) {
			return
		}

		log.debug { "Waiting for results" }
		val result: MessageResult<K, V> = results.receive()
		log.debug { "Got result $result" }
		when(val res = result.result) {
			ConsumerResult.Success, is ConsumerResult.PermanentFailure -> {
				log.debug { "$res for ${result.message.topicPartition}" }
				val offset = OffsetAndMetadata(result.nextOffset)
				consumer.commitSync(mapOf(result.message.topicPartition to offset))
				log.debug { "Offsets committed for ${result.message.topicPartition}" }
			}
			is ConsumerResult.TransitoryFailure -> {
				log.error { "Got transitory failure when processing job: ${res.message}" }
				work.send(result.message)
			}
		}

		work(scope)
	}
}

private fun <K, V> execute(worker: Worker<K, V>): CoroutineContext {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	val threadCount: UInt = computeThreadCount(kafkaConsumer)
	val scope: CoroutineScope = WorkerScope(threadCount)
	log.debug { "Launching Kafka message poll job" }
	val work: Channel<Message<K, V>> = Channel(500)
	scope.launch {
		while(scope.isActive) {
			kafkaConsumer.poll(Duration.ofSeconds(10))
				.map { Message(it) }
				.onEach { log.debug { "Received message: $it" } }
				.forEach { work.send(it) }
		}
	}
	log.debug { "Launching work distributor" }
	val distributor = WorkDistributor(worker, kafkaConsumer)
	log.debug { "Launching result handler" }
	val resultHandler = ResultHandler(distributor, work, kafkaConsumer)
	distributor.start(scope)
	resultHandler.start(scope)
	return scope.coroutineContext
}

private fun <K, V> KafkaConsumer<K, V>.partitions(): List<PartitionInfo> = this
	.subscription()
	.map { partitionsFor(it) }
	.flatten()
	.also { println("Found partition $it") }
	.toList()

private fun <K, V> computeThreadCount(consumer: KafkaConsumer<K, V>): UInt {
	val partitionCount: Int = consumer.partitions().count()
	return (partitionCount / 8).coerceAtLeast(1).toUInt()
}

private fun min(u0: UInt, u1: UInt): UInt = if(u0 < u1) u0 else u1

private fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.config).apply {
		subscribe(worker.topics)
	}
}

fun <K, V> launchConsumer(
	scope: CoroutineScope,
	worker: Worker<K, V>,
	queue: SendChannel<MessageResult<K, V>>,
	message: Message<K, V>
): Job = scope.launch {
	val result: ConsumerResult = try {
		wait(message.attempts, worker.retryPolicy)
		log.debug { "Processing message $message (attempts: ${message.attempts})" }
		val pipedMessage: Message<K, V> = worker.pipeline.foldMessage(message)
		worker.consume(pipedMessage).also { resultToPipe ->
			worker.pipeline.forEach { it.onResult(resultToPipe) }
		}
	} catch(e: Throwable) {
		worker.pipeline.forEach { it.onException(e) }
		ConsumerResult.TransitoryFailure(e.message)
	}

	log.debug { "Sending $result to result queue" }
	queue.send(MessageResult(result, message.nextAttempt()))
}

suspend fun wait(attemptsDone: Long, retryPolicy: RetryPolicy): Duration = when {
	attemptsDone < 0L -> throw IllegalArgumentException("Argument attemptsDone cannot be negative")
	attemptsDone == 0L -> Duration.ZERO
	else -> retryPolicy.waitTime(attemptsDone).also { delay(it) }
}

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
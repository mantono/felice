@file:Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")

package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import java.time.Duration
import kotlin.coroutines.CoroutineContext

private val log = KotlinLogging.logger("felice-engine")

fun <K, V> Worker<K, V>.start(): CoroutineContext = execute(this)

private fun <K, V> execute(worker: Worker<K, V>): CoroutineContext {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	val threadCount: UInt = computeThreadCount(kafkaConsumer)
	val scope: CoroutineScope = WorkerScope(threadCount)
	scope.launch {
		run(scope, worker, kafkaConsumer)
	}
	return scope.coroutineContext
}
//
//private fun <K, V> createActors(
//	scope: CoroutineScope,
//	kafkaConsumer: KafkaConsumer<K, V>,
//	worker: Worker<K, V>
//): Map<TopicPartition, ConsumerActor<K, V>> {
//	kafkaConsumer.partitions().asSequence()
//		.map {  }
//}

private fun <K, V> KafkaConsumer<K, V>.partitions(): List<PartitionInfo> = this
	.listTopics()
	.flatMap { it.value }
	.toList()

private fun <K, V> computeThreadCount(consumer: KafkaConsumer<K, V>): UInt {
	val partitionCount: Int = consumer.partitions().count()
	return (partitionCount / 8).coerceAtLeast(1).toUInt()
}

private fun min(u0: UInt, u1: UInt): UInt = if(u0 < u1) u0 else u1

private fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.options + ("group.id" to worker.groupId)).apply {
		subscribe(worker.topics)
	}
}

private tailrec suspend fun <K, V> run(
	scope: CoroutineScope,
	//consumeFunction: suspend (Message<K, V>) -> ConsumerResult,
	worker: Worker<K, V>,
	cons: KafkaConsumer<K, V>,
	queue: Channel<MessageResult> = Channel(100)
	//offsets: Map<TopicPartition, Long> = initOffsets(cons)
) {
	if(!scope.isActive) {
		cons.close(Duration.ofSeconds(30))
		queue.close()
		return
	}

	log.debug { "Starting poll" }
	cons.poll(Duration.ofSeconds(10))
		.map { Message(it) }
		.also { log.debug { "Processing message: $it" } }
		//.sortedBy { it.timestamp }
		//.groupBy { "${it.topic}:${it.partition}" }
		.forEach { launchConsumer(scope, worker, queue, it) }

	run(scope, worker, cons, queue)
}

fun <K, V> launchConsumer(
	scope: CoroutineScope,
	worker: Worker<K, V>,
	queue: SendChannel<MessageResult>,
	message: Message<K, V>
): Job = scope.launch {
	val result: ConsumerResult = try {
		val pipedMessage: Message<K, V> = worker.pipeline.foldMessage(message)
		worker.consume(pipedMessage).also { resultToPipe ->
			worker.pipeline.forEach { it.onResult(resultToPipe) }
		}
	} catch(e: Throwable) {
		worker.pipeline.forEach { it.onException(e) }
		ConsumerResult.Retry(e.message)
	}

	queue.send(MessageResult(result, message.topicPartition))
}

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
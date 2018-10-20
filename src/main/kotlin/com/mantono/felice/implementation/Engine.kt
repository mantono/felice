@file:Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")

package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import java.time.Duration
import kotlin.coroutines.CoroutineContext


private fun <K, V> execute(worker: Worker<K, V>): CoroutineContext {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	val threadCount: UInt = computeThreadCount(kafkaConsumer)
	val scope: CoroutineScope = WorkerScope(threadCount)
	scope.launch {
		// TODO do something here, concurrently
	}
	return scope.coroutineContext
}

class WorkerScope(threads: UInt): CoroutineScope {
	override val coroutineContext: CoroutineContext = dispatcher(threads)
}

private fun <K, V> KafkaConsumer<K, V>.partitions(): List<PartitionInfo> = this
	.listTopics()
	.flatMap { it.value }
	.toList()

private fun <K, V> computeThreadCount(consumer: KafkaConsumer<K, V>): UInt {
	val partitionCount: Int = consumer.partitions().count()
	return (partitionCount / 8).coerceAtLeast(1).toUInt()
}

private fun min(u0: UInt, u1: UInt): UInt = if(u0 < u1) u0 else u1

fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.options + ("groupId" to worker.groupId)).apply {
		subscribe(worker.topics)
	}
}

private tailrec suspend fun <K, V> run(
	scope: CoroutineScope,
	consumeFunction: suspend (Message<K, V>) -> ConsumerResult,
	cons: KafkaConsumer<K, V>,
	queue: Channel<Message<K, V>> = Channel(100)
) {
	if(!scope.isActive) {
		return
	}
	cons.poll(Duration.ofSeconds(20))
		.map {
			val headers = it.headers().map { it.key() to it.value() }.toMap()
			Message(it.key(), it.value(), it.topic(), headers, it.offset(), it.partition())
		}
		.forEach { queue.send(it) }

	queue.receiveOrNull()?.let {
		val result = consumeFunction(it)
		val offsets
		cons.commitSync()
		println(result)
	}

	run(scope, consumeFunction, cons, queue)
}

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
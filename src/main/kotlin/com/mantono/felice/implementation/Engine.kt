@file:Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")

package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.RetryPolicy
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo
import java.time.Duration
import java.util.concurrent.Semaphore
import kotlin.coroutines.CoroutineContext

private val log = KotlinLogging.logger("felice-engine")

fun <K, V> Worker<K, V>.start(): CoroutineContext = execute(this)

private fun <K, V> execute(worker: Worker<K, V>): CoroutineContext {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	val threadCount: UInt = computeThreadCount(kafkaConsumer)
	val scope: CoroutineScope = WorkerScope(threadCount)
	log.debug { "Launching director" }
	val director = KafkaDirector(kafkaConsumer, worker)
	log.debug { "Launching work distributor" }
	director.start(scope)
	return scope.coroutineContext
}

internal fun <K, V> KafkaConsumer<K, V>.partitions(): List<PartitionInfo> = this
	.subscription()
	.map { partitionsFor(it) }
	.flatten()
	.also { println("Found partition $it") }
	.toList()

private fun <K, V> computeThreadCount(consumer: KafkaConsumer<K, V>): UInt {
	val partitionCount: Int = consumer.partitions().count()
	return (partitionCount / 8).coerceAtLeast(2).toUInt()
}

private fun min(u0: UInt, u1: UInt): UInt = if(u0 < u1) u0 else u1

private fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.config).apply {
		subscribe(worker.topics)
	}
}

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
@file:Suppress("EXPERIMENTAL_API_USAGE", "EXPERIMENTAL_UNSIGNED_LITERALS")

package com.mantono.felice.implementation

import com.mantono.felice.api.worker.Connection
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlin.coroutines.CoroutineContext

private val log = KotlinLogging.logger("felice-engine")

fun <K, V> Worker<K, V>.start(): CoroutineContext = execute(this)

private fun <K, V> execute(worker: Worker<K, V>): CoroutineContext {
	val kafkaConsumer: Connection<K, V> = createKafkaConsumer(worker)
	val threadCount: UInt = computeThreadCount(kafkaConsumer)
	val scope: CoroutineScope = WorkerScope(threadCount)
	log.debug { "Launching director" }
	val director = KafkaDirector(kafkaConsumer, worker)
	log.debug { "Launching work distributor" }
	director.start(scope)
	return scope.coroutineContext
}

private fun <K, V> computeThreadCount(consumer: Connection<K, V>): UInt {
	val partitionCount: Int = runBlocking { consumer.partitions() }
	return (partitionCount / 8).coerceAtLeast(2).toUInt()
}

private fun min(u0: UInt, u1: UInt): UInt = if(u0 < u1) u0 else u1

private fun <K, V> createKafkaConsumer(worker: Worker<K, V>): Connection<K, V> {
	val kc = KafkaConsumer<K, V>(worker.config).apply {
		subscribe(worker.topics)
	}
	return KafkaConnection(kc, CoroutineScope(Job()))
}

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumeResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration


private fun <K, V> execute(worker: Worker<K, V>): Job = GlobalScope.launch {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	run(this, worker.consumer::consume, kafkaConsumer)
}

fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.options + ("groupId" to worker.groupId)).apply {
		subscribe(worker.topics)
	}
}

private tailrec suspend fun <K, V> run(
	scope: CoroutineScope,
	consumeFunction: suspend (com.mantono.felice.api.Message<K, V>) -> ConsumeResult,
	cons: KafkaConsumer<K, V>,
	queue: Channel<com.mantono.felice.api.Message<K, V>> = Channel(100)
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
package com.mantono.felice.implementation

import com.mantono.felice.api.Consumer
import com.mantono.felice.api.Interceptor
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
import java.util.concurrent.atomic.AtomicReference

data class FeliceWorker<K, V>(
	override val groupId: String,
	override val topics: Set<String>,
	override val options: Map<String, Any>,
	override val interceptors: List<Interceptor<K, V>>,
	override val consumer: Consumer<K, V>
): Worker<K, V> {
	private val job: AtomicReference<Job> = AtomicReference()

	override fun start(): Job = job.updateAndGet { current: Job? ->
		current ?: execute(this)
	}
}

private fun <K, V> execute(worker: Worker<K, V>): Job = GlobalScope.launch {
	val kafkaConsumer: KafkaConsumer<K, V> = createKafkaConsumer(worker)
	run(this, worker, kafkaConsumer)
}

fun <K, V> createKafkaConsumer(worker: Worker<K, V>): KafkaConsumer<K, V> {
	return KafkaConsumer<K, V>(worker.options + ("groupId" to worker.groupId)).apply {
		subscribe(worker.topics)
	}
}

private tailrec suspend fun <K, V> run(
	scope: CoroutineScope,
	worker: Worker<K, V>,
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
		val result = worker.consumer.consume(it)
		println(result)
	}

	run(scope, worker, cons, queue)
}

private data class Message<K, V>(
	override val key: K?,
	override val value: V?,
	override val topic: String,
	override val headers: Map<String, Any>,
	override val offset: Long,
	override val partition: Int
): Message<K, V>

internal fun Boolean.onTrue(execute: () -> Unit): Boolean {
	return if(this) {
		execute()
		true
	} else {
		false
	}
}
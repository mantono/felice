package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumeResult
import com.mantono.felice.api.MessageConsumer
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
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

data class FeliceWorker<K, V>(
	override val groupId: String,
	override val topics: Set<String>,
	override val options: Map<String, Any>,
	override val interceptors: List<Interceptor<K, V>>,
	override val consumer: MessageConsumer<K, V>
): Worker<K, V> {
	private val job: AtomicReference<Job> = AtomicReference()

	override fun start(): Job = job.updateAndGet { current: Job? ->
		current ?: execute(this)
	}
}
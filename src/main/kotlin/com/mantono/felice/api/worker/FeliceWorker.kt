package com.mantono.felice.api.worker

import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.Interceptor
import kotlinx.coroutines.Job
import org.apache.kafka.common.serialization.Deserializer

class FeliceWorker<K, V>(
	override val groupId: String,
	override val topics: Set<String>,
	override val options: Map<String, Any>,
	override val pipeline: List<Interceptor>,
	override val deserializeKey: Deserializer<K>,
	override val deserializeValue: Deserializer<V>,
	consumer: MessageConsumer<K, V>
): Worker<K, V>, MessageConsumer<K, V> by consumer
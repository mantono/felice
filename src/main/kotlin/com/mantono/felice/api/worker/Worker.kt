package com.mantono.felice.api.worker

import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.MessageConsumer
import org.apache.kafka.common.serialization.Deserializer

interface Worker<K, V>: MessageConsumer<K, V> {
	val topics: Set<String>
	val groupId: String
	val options: Map<String, Any>
	val interceptors: List<Interceptor<K, V>>

	val deserializeKey: Deserializer<K>
	val deserializeValue: Deserializer<V>
}
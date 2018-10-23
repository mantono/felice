package com.mantono.felice.api.worker

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageConsumer
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer

interface Worker<K, V>: MessageConsumer<K, V> {
	val topics: Set<String>
	val groupId: String
	val options: Map<String, Any>
	val pipeline: List<Interceptor>

	val deserializeKey: Deserializer<K>
	val deserializeValue: Deserializer<V>
}
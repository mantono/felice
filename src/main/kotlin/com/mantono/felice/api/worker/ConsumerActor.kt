package com.mantono.felice.api.worker

import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.MessageResult
import org.apache.kafka.common.TopicPartition

interface ConsumerActor<K, V>: MessageConsumer<K, V>, DuplexChannel<Message<K, V>, MessageResult<K, V>> {
	val topicPartition: TopicPartition
	val pipeline: List<Interceptor>
}
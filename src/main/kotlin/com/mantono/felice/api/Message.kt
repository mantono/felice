package com.mantono.felice.api

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers

data class Message<K, V>(
	val key: K?,
	val value: V?,
	val timestamp: Long,
	val topic: String,
	val headers: Map<String, ByteArray>,
	val offset: Long,
	val partition: Int
) {
	val topicPartition = TopicPartition(topic, partition)

	constructor(
		key: K?,
		value: V?,
		timestamp: Long,
		topic: String,
		headers: Headers,
		offset: Long,
		partition: Int
	): this(key, value, timestamp, topic, headers.toMap(), offset, partition)

	constructor(message: ConsumerRecord<K, V>): this(
		key = message.key(),
		value = message.value(),
		timestamp = message.timestamp(),
		topic = message.topic(),
		headers = message.headers().toMap(),
		offset = message.offset(),
		partition = message.partition()
	)
}

private fun Headers.toMap(): Map<String, ByteArray> = this
	.map { it.key() to it.value() }
	.toMap()
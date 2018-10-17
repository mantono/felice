package com.mantono.felice.api

import org.apache.kafka.common.header.Headers

data class Message<K, V>(
	val key: K?,
	val value: V?,
	val topic: String,
	val headers: Map<String, ByteArray>,
	val offset: Long,
	val partition: Int
) {
	constructor(
		key: K?,
		value: V?,
		topic: String,
		headers: Headers,
		offset: Long,
		partition: Int
	): this(key, value, topic, headers.toMap(), offset, partition)
}

private fun Headers.toMap(): Map<String, ByteArray> = this
	.map { it.key() to it.value() }
	.toMap()
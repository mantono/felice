package com.mantono.felice.api

import java.time.Instant

data class MessageResult<K, V>(
	val result: ConsumerResult,
	val message: Message<K, V>,
	val timestamp: Instant = Instant.now()
) {
	val nextOffset: Long = message.offset + 1
}
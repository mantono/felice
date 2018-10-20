package com.mantono.felice.api

interface MessageConsumer<K, V> {
	suspend fun consume(message: Message<K, V>): ConsumerResult
}
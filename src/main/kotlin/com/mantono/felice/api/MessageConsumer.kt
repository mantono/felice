package com.mantono.felice.api

interface MessageConsumer<K, V> {
	suspend fun consume(message: Message<K, V>): ConsumeResult
}

interface ValueConsumer<V, T> {
	fun serialize(rawValue: V): T
	suspend fun consumer(value: T): ConsumeResult
}
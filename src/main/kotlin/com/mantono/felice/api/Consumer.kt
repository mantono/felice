package com.mantono.felice.api

interface Consumer<K, V> {
	suspend fun consume(message: Message<K, V>): ConsumeResult
}




package com.mantono.felice.api.worker

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import java.time.Duration

interface Connection<K, V> {
	suspend fun poll(duration: Duration): Sequence<Message<K, V>>
	suspend fun commit(result: MessageResult)
	suspend fun close(timeout: Duration = Duration.ofSeconds(30))
	suspend fun partitions(): Int
}
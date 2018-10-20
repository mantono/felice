package com.mantono.felice.api.worker

import com.mantono.felice.api.Message

interface Pipeline<K, V> {
	fun pipe(message: Message<K, V>): Message<K, V>
}
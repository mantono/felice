package com.mantono.felice.api

import kotlinx.coroutines.Job

interface Worker<K, V> {
	val topics: Set<String>
	val groupId: String
	val options: Map<String, Any>
	val interceptors: List<Interceptor<K, V>>

	fun start(): Job
}

interface MessageWorker<K, V>: Worker<K, V> {
	val consumer: MessageConsumer<K, V>
}

interface ValueWorker<K, V, T>: Worker<K, V> {
	val consumer: ValueConsumer<V, T>
}
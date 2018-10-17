package com.mantono.felice.api

import kotlinx.coroutines.Job

interface Worker<K, V> {
	val topics: Set<String>
	val groupId: String
	val options: Map<String, Any>
	val interceptors: List<Interceptor<K, V>>
	val consumer: MessageConsumer<K, V>

	fun start(): Job
}
package com.mantono.felice.api

interface WorkerBuilder<K, V> {
	fun topic(vararg topics: String): WorkerBuilder<K, V>
	fun groupId(groupId: String): WorkerBuilder<K, V>
	fun option(key: String, value: String): WorkerBuilder<K, V>
	fun options(options: Map<String, Any>): WorkerBuilder<K, V>
	fun interceptor(interceptor: Interceptor<K, V>): WorkerBuilder<K, V>
	fun consumer(consumer: MessageConsumer<K, V>): Worker<K, V>
}
package com.mantono.felice.api

import com.mantono.felice.api.worker.FeliceWorker
import com.mantono.felice.api.worker.Worker

data class WorkerBuilder<K, V>(
	val topics: List<String> = emptyList(),
	val options: Map<String, Any> = emptyMap(),
	val keyDeserializer: ((ByteArray) -> K)? = null,
	val valueDeserializer: ((ByteArray) -> V)? = null,
	val interceptors: List<Interceptor<K, V>> = emptyList()
) {
	fun groupId(groupId: String): WorkerBuilder<K, V> {
		require(groupId.isNotBlank())
		val groupOption: Pair<String, String> = "groupId" to groupId
		return copy(options = options + groupOption)
	}
	fun topic(vararg topics: String): WorkerBuilder<K, V> = copy(topics = this.topics + topics)

	fun option(key: String, value: String): WorkerBuilder<K, V> =
		copy(options = this.options + (key to value))

	fun options(options: Map<String, Any>): WorkerBuilder<K, V>  =
		copy(options = this.options + options)

	fun interceptor(interceptor: Interceptor<K, V>): WorkerBuilder<K, V> =
		copy(interceptors = this.interceptors + interceptor)

	fun consumer(consumer: MessageConsumer<K, V>): Worker<K, V> {
		check(topics.isNotEmpty()) { "At least one topic must be given" }
		check(options.contains("groupId")) { "Group id was not set" }
		return this.toWorker(consumer)
	}

	fun build(): Wo

	private fun toWorker(consumer: MessageConsumer<K, V>): Worker<K, V> = FeliceWorker(
		groupId = options["groupId"].toString(),
		topics = topics.toSet(),
		interceptors = interceptors,
		options = options,
		consumer = consumer
	)
}
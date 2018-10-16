package com.mantono.felice.implementation

import com.mantono.felice.api.Consumer
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.Worker
import com.mantono.felice.api.WorkerBuilder

data class FeliceWorkerBuilder<K, V>(
	private val topics: List<String> = emptyList(),
	private val groupId: String? = null,
	private val options: Map<String, Any> = emptyMap(),
	private val interceptors: List<Interceptor<K, V>> = emptyList()
): WorkerBuilder<K, V> {

	override fun groupId(groupId: String): WorkerBuilder<K, V> = copy(groupId = groupId)
	override fun topic(vararg topics: String): WorkerBuilder<K, V> = copy(topics = this.topics + topics)

	override fun option(key: String, value: String): WorkerBuilder<K, V> =
		copy(options = this.options + (key to value))

	override fun options(options: Map<String, Any>): WorkerBuilder<K, V>  =
		copy(options = this.options + options)

	override fun interceptor(interceptor: Interceptor<K, V>): WorkerBuilder<K, V> =
		copy(interceptors = this.interceptors + interceptor)

	override fun consumer(consumer: Consumer<K, V>): Worker<K, V> {
		check(topics.isNotEmpty()) { "At least one topic must be given" }
		check(groupId != null) { "Group id was not set" }
		return this.toWorker(consumer)
	}

	private fun toWorker(consumer: Consumer<K, V>): Worker<K, V> = FeliceWorker(
		groupId = groupId!!,
		topics = topics.toSet(),
		interceptors = interceptors,
		options = options,
		consumer = consumer
	)
}
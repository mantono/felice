package com.mantono.felice.api

import com.mantono.felice.api.worker.FeliceWorker
import com.mantono.felice.api.worker.Worker
import org.apache.kafka.common.serialization.Deserializer

private val defaultOptions: Map<String, Any> = mapOf(
	"enable.auto.commit" to false,
	"bootstrap.server" to "localhost:9092"
)

data class WorkerBuilder<K, V>(
	private val topics: List<String> = emptyList(),
	private val options: Map<String, Any> = defaultOptions,
	private val pipeline: List<Interceptor> = emptyList(),
	private val keyDeserializer: Deserializer<K>? = null,
	private val valueDeserializer: Deserializer<V>? = null,
	private val consumer: MessageConsumer<K, V>? = null
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

	fun intercept(interceptor: Interceptor): WorkerBuilder<K, V> =
		copy(pipeline = this.pipeline + interceptor)

	fun consumer(consumer: MessageConsumer<K, V>): WorkerBuilder<K, V> =
		copy(consumer = consumer)

	fun consumer(consumer: suspend (Message<K, V>) -> ConsumerResult): WorkerBuilder<K, V> {
		val consumerObject = object: MessageConsumer<K, V> {
			override suspend fun consume(message: Message<K, V>): ConsumerResult = consumer(message)
		}
		return copy(consumer = consumerObject)
	}

	fun deserializerKey(deserializer: Deserializer<K>): WorkerBuilder<K, V> =
		this.copy(keyDeserializer = deserializer)

	fun deserializeKey(deserializer: (ByteArray?) -> K): WorkerBuilder<K, V> =
		this.copy(keyDeserializer = createDeserializer(deserializer))

	fun deserializerValue(deserializer: Deserializer<V>): WorkerBuilder<K, V> =
		this.copy(valueDeserializer = deserializer)

	fun deserializeValue(deserializer: (ByteArray?) -> V): WorkerBuilder<K, V> =
		this.copy(valueDeserializer = createDeserializer(deserializer))

	fun build(): Worker<K, V> {
		verifyState()
		return toWorker()
	}

	private fun toWorker(): Worker<K, V> =
		FeliceWorker(
			groupId = options["groupId"].toString(),
			topics = topics.toSet(),
			pipeline = pipeline,
			options = options,
			deserializeKey = keyDeserializer!!,
			deserializeValue = valueDeserializer!!,
			consumer = consumer!!
		)

	private fun verifyState() {
		check(topics.isNotEmpty()) { "At least one topic must be given" }
		check(options.contains("groupId")) { "Group id was not set" }
		check(keyDeserializer != null) { "Deserializer for key is not set" }
		check(valueDeserializer != null) { "Deserializer for value is not set" }
		check(consumer != null) { "MessageConsumer is not set" }
	}
}

private fun <T> createDeserializer(deserializer: (ByteArray?) -> T): Deserializer<T> {
	return object: Deserializer<T> {
		override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
		override fun deserialize(topic: String?, data: ByteArray?): T = deserializer(data)
		override fun close() {}
	}
}
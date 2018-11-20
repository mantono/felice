package com.mantono.felice.api

import com.mantono.felice.api.worker.KafkaConfig
import com.mantono.felice.api.worker.Worker
import org.apache.kafka.common.serialization.Deserializer
import java.time.Duration

data class WorkerBuilder<K, V>(
	private val topics: List<String> = emptyList(),
	private val config: Map<String, Any> = KafkaConfig.Consumer.default,
	private val pipeline: List<Interceptor> = emptyList(),
	private val keyDeserializer: Deserializer<K>? = null,
	private val valueDeserializer: Deserializer<V>? = null,
	private val consumer: MessageConsumer<K, V>? = null,
	private val retryPolicy: RetryPolicy = Infinite,
	private val timeout: Duration = Duration.ZERO
) {
	fun groupId(groupId: String): WorkerBuilder<K, V> {
		require(groupId.isNotBlank())
		val groupOption: Pair<String, String> = "group.id" to groupId
		return copy(config = config + groupOption)
	}

	fun topic(vararg topics: String): WorkerBuilder<K, V> = copy(topics = this.topics + topics)

	fun host(vararg hosts: String): WorkerBuilder<K, V> =
		copy(config = this.config + ("bootstrap.servers" to hosts.toList()))

	fun option(key: String, value: String): WorkerBuilder<K, V> =
		copy(config = this.config + (key to value))

	fun config(config: Map<String, Any>): WorkerBuilder<K, V>  =
		copy(config = this.config + config)

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

	fun retryPolicy(retryPolicy: RetryPolicy): WorkerBuilder<K, V> =
		this.copy(retryPolicy = retryPolicy)

	fun timeout(timeout: Duration): WorkerBuilder<K, V> = this.copy(timeout = timeout).also {
		require(!this.timeout.isNegative)
	}

	fun build(): Worker<K, V> {
		verifyState()
		return toWorker()
	}

	private fun toWorker(): Worker<K, V> =
		Worker(
			topics = topics.toSet(),
			pipeline = pipeline,
			config = config,
			deserializeKey = keyDeserializer!!,
			deserializeValue = valueDeserializer!!,
			retryPolicy = retryPolicy,
			timeout = timeout,
			consumer = consumer!!
		)

	private fun verifyState() {
		check(topics.isNotEmpty()) { "At least one topic must be given" }
		check(keyDeserializer != null) { "Deserializer for key is not set" }
		check(valueDeserializer != null) { "Deserializer for value is not set" }
		check(consumer != null) { "MessageConsumer is not set" }
		KafkaConfig.Consumer.required.forEach {
			require(it in config.keys) {
				"Missing value for required consumer config setting: $it"
			}
		}
	}
}

private fun <T> createDeserializer(deserializer: (ByteArray?) -> T): Deserializer<T> {
	return object: Deserializer<T> {
		override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
		override fun deserialize(topic: String?, data: ByteArray?): T = deserializer(data)
		override fun close() {}
	}
}
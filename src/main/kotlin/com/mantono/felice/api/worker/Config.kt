package com.mantono.felice.api.worker

sealed class KafkaConfig {
	abstract val required: Set<String>
	abstract val default: Map<String, Any>

	object Consumer: KafkaConfig() {
		override val required: Set<String> = setOf(
			"bootstrap.servers",
			"group.id",
			"key.deserializer",
			"value.deserializer"
		)

		override val default: Map<String, Any> = mapOf(
			"enable.auto.commit" to false,
			"bootstrap.servers" to "localhost:9092",
			"key.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
			"value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
			"heartbeat.interval.ms" to 4_000,
			"session.timeout.ms" to 12_000,
			"auto.offset.reset" to "earliest",
			"max.poll.records" to 100
		)
	}

	object Producer: KafkaConfig() {
		override val required: Set<String> = setOf(
			"bootstrap.servers",
			"key.serializer",
			"value.serializer"
		)

		override val default: Map<String, Any> = mapOf(
			"bootstrap.servers" to "localhost:9092",
			"key.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
			"value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
			"acks" to "all",
			"retries" to 5,
			"linger.ms" to 100,
			"reconnect.backoff.ms" to 500,
			"reconnect.backoff.max.ms" to 30_000,
			"retry.backoff.ms" to 200

		)
	}
}
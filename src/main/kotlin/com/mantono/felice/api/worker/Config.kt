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
			"value.deserializer" to "org.apache.kafka.common.serialization.ByteArrayDeserializer"
		)
	}

	object Producer: KafkaConfig() {
		override val default: Map<String, Any> = emptyMap()
		override val required: Set<String> = emptySet()
	}
}
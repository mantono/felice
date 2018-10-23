package com.mantono.felice

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.WorkerBuilder
import com.mantono.felice.api.worker.Worker
import com.mantono.felice.implementation.start
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.time.delay
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.coroutines.CoroutineContext

class WorkerBuildetTest {

	@Test
	fun testNormalControlFlow() {

		val interceptor = object: Interceptor {
			override fun <K, V> intercept(message: Message<K, V>): Message<K, V> {
				println("Intercepted message $message")
				return message
			}
		}

		val producerOptions = mapOf<String, String>(
			"bootstrap.servers" to "kafka:9092",
			"key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
			"value.serializer" to "org.apache.kafka.common.serialization.StringSerializer"
		)

		val prod = KafkaProducer<String, String>(producerOptions)
		prod.send(ProducerRecord("topic1", "1", "Test"))
		prod.send(ProducerRecord("topic1", "1", "Testd"))
		prod.send(ProducerRecord("topic1", "2", "Testw"))
		prod.send(ProducerRecord("topic1", "1", "Tests"))
		prod.send(ProducerRecord("topic1", "2", "Tests"))
		prod.send(ProducerRecord("topic1", "1", "Testx"))

		Thread.sleep(400)

		val worker: Worker<String, String> = WorkerBuilder<String, String>()
			.topic("topic1", "topic2")
			.groupId("my-groupId")
			.option("bootstrap.servers", "kafka:9092")
			.deserializeKey { String(it!!) }
			.deserializeValue { String(it!!) }
			.intercept(interceptor)
			.consumer {
				println("${it.topic} / ${it.partition} / ${it.offset}")
				ConsumerResult.Succes
			}
			.build()

		val context: CoroutineContext = worker.start()

		Thread.sleep(1000L)
		assertTrue(context.isActive)
		assertTrue(context.cancel())
		assertFalse(context.isActive)
	}


}
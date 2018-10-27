package com.mantono.felice

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.WorkerBuilder
import com.mantono.felice.api.worker.Worker
import com.mantono.felice.implementation.start
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.coroutines.CoroutineContext

class WorkerBuilderTest {

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

		val rand = Random()
		val prod = KafkaProducer<String, String>(producerOptions)
		prod.send(ProducerRecord("topic1", rand.nextInt().toString(), rand.nextInt().toString()))
		prod.send(ProducerRecord("topic1", rand.nextInt().toString(), rand.nextInt().toString()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString(), rand.nextInt().toString()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString(), rand.nextInt().toString()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString(), rand.nextInt().toString()))
		prod.send(ProducerRecord("topic1", rand.nextInt().toString(), rand.nextInt().toString()))

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
				ConsumerResult.Success
			}
			.build()

		val context: CoroutineContext = worker.start()

		Thread.sleep(35_000L)
		assertTrue(context.isActive)
		context.cancel()
		assertFalse(context.isActive)
	}


}
package com.mantono.felice

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.Limited
import com.mantono.felice.api.Message
import com.mantono.felice.api.WorkerBuilder
import com.mantono.felice.api.worker.KafkaConfig
import com.mantono.felice.api.worker.Worker
import com.mantono.felice.implementation.start
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

class WorkerBuilderTest {

	@Test
	fun testNormalControlFlow() {

		val interceptor = object: Interceptor {
			val x = AtomicReference<Int>()
			override fun <K, V> intercept(message: Message<K, V>): Message<K, V> {
				println("Intercepted message $message")
				return message
			}
		}

		val producerOptions = KafkaConfig.Producer.default + ("bootstrap.servers" to "kafka:9092")

			/*mapOf<String, String>(
			"bootstrap.servers" to "kafka:9092",
			"key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
			"value.serializer" to "org.apache.kafka.common.serialization.StringSerializer"
		)*/

		val rand = Random()
		val prod = KafkaProducer<ByteArray, ByteArray>(producerOptions)

		val worker: Worker<String, String> = WorkerBuilder<String, String>()
			.topic("topic1", "topic2")
			.groupId("my-groupId")
			.host("kafka:9092")
			.deserializeKey { String(it!!).also { println(it.toString()) } }
			.deserializeValue { String(it!!) }
			.intercept(interceptor) {
				x.getAndSet(4)
			}
			.retryPolicy(Limited(5, Duration.ofSeconds(1), Duration.ofMillis(500)))
			.consumer {
				println("${it.topic} / ${it.partition} / ${it.offset}")
				delay(5L + Random().nextInt(210))
				if(System.currentTimeMillis() % 17L == 0L)
					return@consumer ConsumerResult.TransitoryFailure("Noooo")
				ConsumerResult.Success
			}
			.build()

		val context: CoroutineContext = worker.start()

		prod.send(ProducerRecord("topic1", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic1", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic1", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))

		Thread.sleep(14_000L)

		prod.send(ProducerRecord("topic2", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic2", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))
		prod.send(ProducerRecord("topic1", rand.nextInt().toString().toByteArray(), rand.nextInt().toString().toByteArray()))


		Thread.sleep(465_000L)
		assertTrue(context.isActive)
		context.cancel()
		assertFalse(context.isActive)
	}
}
package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.worker.Connection
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration

private val log = KotlinLogging.logger("kafka-connection")

class KafkaConnection<K, V>(private val consumer: KafkaConsumer<K, V>): Connection<K, V> {
	private val mutex = Mutex()

	override suspend fun poll(duration: Duration): Sequence<Message<K, V>> {
		return mutex.onAcquire {
			consumer
				.poll(duration)
				.map { Message(it) }
				.asSequence()
		}
	}

	override suspend fun commit(result: MessageResult) {
		val metadata = OffsetAndMetadata(result.offset + 1)
		val offset: Map<TopicPartition, OffsetAndMetadata> = mapOf(result.topicPartition to metadata)
		mutex.onAcquire {
			consumer.commitSync(offset, Duration.ofSeconds(10))
			log.info { "Successfully committed: $offset" }
		}
	}

	override suspend fun close(timeout: Duration) {
		mutex.onAcquire { consumer.close(timeout) }
	}

	override suspend fun partitions(): Int = mutex.onAcquire {
		consumer.subscription()
			.map { consumer.partitionsFor(it) }
			.flatten()
			.also { println("Found partition $it") }
			.toList()
			.count()
	}
}

suspend fun <T> Mutex.onAcquire(block: suspend () -> T): T {
	try {
		this.lock()
		return block()
	} finally {
		this.unlock()
	}
}
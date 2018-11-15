package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.worker.Connection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.time.delay
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.Semaphore

private val log = KotlinLogging.logger("kafka-connection")

private typealias PartitionOffset = Pair<TopicPartition, OffsetAndMetadata>

class KafkaConnection<K, V>(
	private val consumer: KafkaConsumer<K, V>,
	offsetsBuffer: Int = 1_000
): Connection<K, V> {
	private val mutex = Mutex()
	private val offsets = Channel<PartitionOffset>(offsetsBuffer)
	private val initiateCommitting = Semaphore(1)

	override suspend fun poll(duration: Duration): Sequence<Message<K, V>> {
		return mutex.onAcquire {
			consumer
				.poll(duration)
				.map { Message(it) }
				.asSequence()
		}
	}

	override suspend fun commit(result: MessageResult) {
		val offset = OffsetAndMetadata(result.offset + 1)
		offsets.send(result.topicPartition to offset)
		initiateCommitting.tryAcquire().onTrue {
			GlobalScope.launch {
				pollOffsets(this)
			}
		}
	}

	private tailrec suspend fun pollOffsets(
		scope: CoroutineScope,
		polledOffsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap(8),
		pollAttempts: Int = 0
	) {
		if(!scope.isActive) {
			log.info { "Offset committer exits" }
			offsets.close()
			return
		}

		delay(Duration.ofMillis(50L))
		offsets.poll()?.let { polledOffsets.put(it.first, it.second) }
		if(pollAttempts > 100 && polledOffsets.isNotEmpty()) {
			mutex.onAcquire {
				log.debug { "Acquired mutex" }
				consumer.commitSync(polledOffsets, Duration.ofSeconds(10))
				polledOffsets.forEach { topicPartition, offsetAndMetadata ->
					log.info { "Committing $topicPartition / ${offsetAndMetadata.offset()}" }
				}
			}
			pollOffsets(scope, HashMap(polledOffsets.size), 0)
		} else {
			log.debug { "Received offsets: ${polledOffsets.size}, poll attempts: $pollAttempts" }
			pollOffsets(scope, polledOffsets, pollAttempts + 1)
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
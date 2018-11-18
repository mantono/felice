package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.worker.Connection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.time.withTimeout
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.concurrent.Semaphore

private val log = KotlinLogging.logger("kafka-connection")

class KafkaConnection<K, V>(
	private val consumer: KafkaConsumer<K, V>,
	private val scope: CoroutineScope,
	private val commitInterval: Duration = Duration.ofSeconds(30)
): Connection<K, V> {
	private val consumerMutex = Mutex()
	private val offsets: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap(16)
	private val offsetsMutex = Mutex()
	private val initiateCommitting = Semaphore(1)

	override suspend fun poll(duration: Duration): Sequence<Message<K, V>> {
		return consumerMutex.onAcquire {
			consumer
				.poll(duration)
				.map { Message(it) }
				.asSequence()
		}
	}

	override suspend fun commit(result: MessageResult) {
		offsetsMutex.onAcquire {
			offsets[result.topicPartition] = OffsetAndMetadata(result.offset + 1)
		}
		initiateCommitting.tryAcquire().onTrue {
			scope.launch {
				pollOffsets(this)
			}
		}
	}

	private tailrec suspend fun pollOffsets(scope: CoroutineScope) {
		if(!scope.isActive) {
			log.info { "Offset committer exits" }
			initiateCommitting.release()
			return
		}

		try {
			delay(commitInterval)
			if(offsets.isNotEmpty()) {
				log.debug { "Will commit offsets" }
				onAcquire(offsetsMutex, consumerMutex) {
					log.debug { "Acquired mutexes" }
					consumer.commitSync(offsets, Duration.ofSeconds(10))
					log.debug { "Committed offsets: $offsets" }
					offsets.clear()
				}
			} else {
				log.debug { "No offsets to commit" }
			}
		} catch(e: Throwable) {
			e.printStackTrace()
		}

		pollOffsets(scope)
	}

	override suspend fun close(timeout: Duration) {
		consumerMutex.onAcquire { consumer.close(timeout) }
	}

	override suspend fun partitions(): Int = consumerMutex.onAcquire {
		consumer.subscription()
			.map { consumer.partitionsFor(it) }
			.flatten()
			.also { log.debug { "Found partition $it" } }
			.toList()
			.count()
	}
}

suspend fun <T> Mutex.onAcquire(block: suspend () -> T): T {
	try {
		this.lock(block)
		return block()
	} finally {
		if(this.holdsLock(block)) {
			this.unlock(block)
		}
	}
}

suspend fun <T> onAcquire(
	vararg mutexes: Mutex,
	timeout: Duration = Duration.ofSeconds(60),
	block: suspend () -> T
): T {
	try {
		return withTimeout(timeout) {
			mutexes.forEach { it.lock(block) }
			block()
		}
	} finally {
		mutexes
			.filter { it.holdsLock(block) }
			.forEach { it.unlock() }
	}
}
package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import mu.KotlinLogging
import org.apache.kafka.common.TopicPartition

interface PartitionedSendChannel<V> {
	fun offer(value: V): Boolean
	suspend fun send(value: V)
}

private val log = KotlinLogging.logger("actor-swarm")

class ActorSwarm<K, V>(
	private val worker: Worker<K, V>,
	private val resultsChannel: SendChannel<MessageResult>,
	private val scope: CoroutineScope,
	private val capacity: Int = 500
): PartitionedSendChannel<Message<K, V>> {
	private val createChannel = Mutex()

	private val channels: MutableMap<TopicPartition, SendChannel<Message<K, V>>> =
		HashMap(64)

	private suspend fun get(topicPartition: TopicPartition): SendChannel<Message<K, V>> {
		return channels[topicPartition] ?: createChannel(topicPartition)
	}

	private suspend fun createChannel(topicPartition: TopicPartition): SendChannel<Message<K, V>> {
		createChannel.lock()
		return channels.computeIfAbsent(topicPartition) {
			scope.launchConsumer(
				worker = worker,
				results = resultsChannel,
				capacity = capacity
			)
		}.also {
			createChannel.unlock()
			log.info { "Created actor for $topicPartition" }
		}
	}

	override fun offer(value: Message<K, V>): Boolean = runBlocking {
		get(value.topicPartition).offer(value)
	}

	override suspend fun send(value: Message<K, V>) {
		get(value.topicPartition).send(value).also {
			log.debug { "Sent message to actor for ${value.topicPartition}" }
		}
	}

}
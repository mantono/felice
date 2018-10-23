package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Interceptor
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.ConsumerActor
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import org.apache.kafka.common.TopicPartition
import java.lang.IllegalStateException

interface Callback {
	suspend fun <K, V> onCompletion(topicPartition: TopicPartition, message: Message<K, V>, result: ConsumerResult)
	suspend fun <K, V> onException(topicPartition: TopicPartition, message: Message<K, V>, exception: Throwable)
}

suspend fun <K, V> executeJob(worker: Worker<K, V>, message: Message<K, V>, callback: Callback) {
	val interceptedMessage: Message<K, V> = worker.pipeline.foldMessage(message)

	try {
		val result: ConsumerResult = worker.consume(interceptedMessage)
		worker.pipeline.forEach { it.onResult(result) }
		callback.onCompletion(message.topicPartition, interceptedMessage, result)
	} catch(exception: Throwable) {
		worker.pipeline.forEach { it.onException(exception) }
		callback.onException(message.topicPartition, interceptedMessage, exception)
	}
}

/*
class FeliceConsumerActor<K, V>(
	override val topicPartition: TopicPartition,
	override val pipeline: List<Interceptor>,
	consumer: MessageConsumer<K, V>,
	private val responses: SendChannel<MessageResult>,
	private val messages: ReceiveChannel<Message<K, V>> = Channel(10)
):
	ConsumerActor<K, V>,
	MessageConsumer<K, V> by consumer,
	SendChannel<MessageResult> by responses,
	ReceiveChannel<Message<K, V>> by messages {

	tailrec suspend fun work() {
		if(isClosedForReceive) {
			return
		}

		val message: Message<K, V> = receive()

		try {

		} catch(e: Throwable) {
			e.printStackTrace()
			pipeline.forEach { it.onException(e) }
			responses.
		}

		work()
	}
}*/

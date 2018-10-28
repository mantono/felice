package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging

private val log = KotlinLogging.logger("actors")

/*fun <K, V> CoroutineScope.workerActor(
	worker: Worker<K, V>,
	results: SendChannel<MessageResult>
): SendChannel<Message<K, V>> {

}*/

/*internal suspend fun <K, V> ReceiveChannel<Message<K, V>>.work(
	workers: Int,
	scope: CoroutineScope,
	worker: Worker<K, V>
): ReceiveChannel<MessageResult> {
	val results = Channel<MessageResult>(workers*10)
	for(i in 1..workers) {
		log.debug { "Launching actor $i" }
		scope.launch {
			work(scope, worker, results)
		}
	}
	return results
}*/

/*internal suspend fun <K, V> work(
	scope: CoroutineScope,
	worker: Worker<K, V>,
	message: Message<K, V>
): Def {
	for(msg in this) {
		log.debug { "Actor received message $msg" }
		if(!scope.isActive) {
			return
		}

		val result: MessageResult = try {
			val interceptedMessage: Message<K, V> = worker.pipeline.foldMessage(msg)
			val consumerResult: ConsumerResult = worker.consume(interceptedMessage)
			worker.pipeline.forEach { it.onResult(consumerResult) }
			MessageResult(consumerResult, msg.topicPartition, msg.offset)
		} catch(e: Throwable) {
			val consumerResult = ConsumerResult.Retry(e.message)
			e.printStackTrace()
			MessageResult(consumerResult, msg.topicPartition, msg.offset)
		}

		log.debug { "Result $msg" }
		results.send(result)
	}
}*/


/*

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
}*/

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

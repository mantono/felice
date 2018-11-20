@file:Suppress("EXPERIMENTAL_API_USAGE")

package com.mantono.felice.implementation

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import com.mantono.felice.api.RetryPolicy
import com.mantono.felice.api.foldMessage
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.time.Duration

private val log = KotlinLogging.logger("actor")

fun <K, V> CoroutineScope.launchConsumer(
	worker: Worker<K, V>,
	results: SendChannel<MessageResult>,
	capacity: Int = 100
): SendChannel<Message<K, V>> = actor(
	context = this.coroutineContext,
	capacity = capacity,
	start = CoroutineStart.DEFAULT,
	onCompletion = { cause: Throwable? ->
		log.error { cause?.message }
	}
) {
	log.info { "started" }
	for(message in channel) {
		log.debug { "Actor received $message" }
		val result: MessageResult = process(worker, message)
		log.debug { "Sending $result to result queue" }
		results.send(result)
	}
}

private tailrec suspend fun <K, V> process(worker: Worker<K, V>, message: Message<K, V>): MessageResult {
	val result: ConsumerResult = try {
		wait(message.attempts, worker.retryPolicy)
		log.debug { "Processing message $message (attempts: ${message.attempts})" }
		val pipedMessage: Message<K, V> = worker.pipeline.foldMessage(message)
		worker.timeout.runWithin {
			worker.consume(pipedMessage)
		}.also { resultToPipe: ConsumerResult ->
			worker.pipeline.forEach { it.onResult(resultToPipe) }
		}
	} catch(e: Throwable) {
		worker.pipeline.forEach { it.onException(e) }
		e.printStackTrace()
		ConsumerResult.TransitoryFailure.fromException(e)
	}

	val messageResult = MessageResult(result, message.topicPartition, message.offset, message.attempts + 1)
	return if(worker.retryPolicy.shouldRetry(messageResult)) {
		process(worker, message.nextAttempt())
	} else {
		messageResult
	}
}

suspend fun <T> Duration.runWithin(timedFunc: suspend () -> T): T {
	return if(this == Duration.ZERO) {
		timedFunc()
	} else {
		withTimeout(this.toMillis()) {
			timedFunc()
		}
	}
}

suspend fun wait(attemptsDone: Long, retryPolicy: RetryPolicy): Duration = when {
	attemptsDone < 0L -> throw IllegalArgumentException("Argument attemptsDone cannot be negative")
	attemptsDone == 0L -> Duration.ZERO
	else -> retryPolicy.waitTime(attemptsDone).also { delay(it) }
}
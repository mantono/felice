package com.mantono.felice.implementation.actors

import com.mantono.felice.implementation.onTrue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.toList
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import mu.KotlinLogging
import java.time.Duration
import java.util.concurrent.Semaphore
import kotlin.coroutines.CoroutineContext

private val log = KotlinLogging.logger("internal-actor")

internal class InternalActor<T>(
	override val coroutineContext: CoroutineContext,
	capacity: Int = Channel.UNLIMITED,
	private val keepAlive: Duration = Duration.ZERO,
	private val onCompletion: (Throwable?) -> Unit = { throwable -> throwable?.let { throw it } },
	private val block: suspend CoroutineScope.(T) -> Unit
): ActorScope<T>, Channel<T> by Channel(capacity) {
	private val start = Semaphore(1)

	fun start(): Actor<T> {
		start.tryAcquire(1).onTrue {
			launch(coroutineContext) {
				try {
					runActor(keepAlive, this@InternalActor, block)
				} catch(e: Throwable) {
					onCompletion(e)
				}
				onCompletion(null)
			}
		}

		return this
	}

	override fun isActive(): Boolean = coroutineContext.isActive

	override fun stop(): Boolean {
		coroutineContext.cancel()
		return this.close()
	}
}

fun <T> CoroutineScope.actor(
	capacity: Int = Channel.UNLIMITED,
	keepAlive: Duration = Duration.ZERO,
	context: CoroutineContext = this.coroutineContext,
	onCompletion: (Throwable?) -> Unit = { throwable -> throwable?.let { throw it } },
	block: suspend CoroutineScope.(T) -> Unit
): Actor<T> {
	return InternalActor(
		coroutineContext = Job() + context,
		capacity = capacity,
		keepAlive = keepAlive,
		onCompletion = onCompletion,
		block = block
	).start()
}

private tailrec suspend fun <T> runActor(
	keepAlive: Duration,
	actorScope: ActorScope<T>,
	block: suspend CoroutineScope.(T) -> Unit
) {
	if(!actorScope.isActive()) {
		actorScope.toList().forEach { actorScope.block(it) }
		log.debug { "Stopping inactive actor" }
		return
	}

	val message: T? = actorScope.receive(keepAlive)
	if(message == null) {
		actorScope.stop()
	} else {
		actorScope.block(message)
	}
	runActor(keepAlive, actorScope, block)
}

suspend fun <T> ReceiveChannel<T>.receive(timeout: Duration): T? {
	return if(timeout === Duration.ZERO) {
		receive()
	} else {
		withTimeoutOrNull(timeout.toMillis()) {
			receive()
		}
	}
}
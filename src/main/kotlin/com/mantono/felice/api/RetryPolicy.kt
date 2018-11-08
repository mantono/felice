package com.mantono.felice.api

import kotlinx.coroutines.time.delay
import java.time.Duration
import java.time.Instant

sealed class RetryPolicy {
	/**
	 * Initial delay between each subsequent attempts when making a retry.
	 * Cannot be negative.
	 */
	abstract val initialBackOff: Duration
	/**
	 * The increase of the delay that is added on top of the initial delay from
	 * [initialBackOff] for each time a job fails with a [ConsumerResult.TransitoryFailure]
	 * Cannot be negative.
	 */
	abstract val buildUp: Duration
	/**
	 * The maximum amount of wait time that can be accumulated from [initialBackOff]
	 * plus [buildUp] time
	 * Cannot be negative.
	 */
	abstract val maxWait: Duration

	fun waitTime(attemptsDone: Long): Duration {
		require(attemptsDone >= 0)
		val accumulatedBuildUp = buildUp.multipliedBy(attemptsDone)
		return initialBackOff.plus(accumulatedBuildUp).coerceAtMost(maxWait)
	}

	fun shouldRetry(result: MessageResult<*, *>): Retry {
		val isNotTransitory: Boolean = result.result !is ConsumerResult.TransitoryFailure
		val hasUsedAllAttempts: Boolean = this is Limited && this.attempts <= result.message.attempts
		val waitTime: Duration = waitTime(result.message.attempts)
		val waitUntil: Instant = result.timestamp.plus(waitTime)

		return when {
			isNotTransitory -> Retry.Never
			hasUsedAllAttempts -> Retry.Never
			waitUntil.isAfter(Instant.now()) -> Retry.Wait(waitUntil)
			else -> Retry.Now
		}
	}
}

sealed class Retry {
	object Now: Retry()
	object Never: Retry()
	data class Wait(val until: Instant): Retry()

	suspend fun waitAndRetry(): Boolean = when(this) {
		Never -> false
		Now -> true
		is Wait -> true.also {
			val waitTime = Duration.between(Instant.now(), until).coerceAtLeast(Duration.ZERO)
			delay(waitTime)
		}
	}
}

object Infinite: RetryPolicy() {
	override val initialBackOff: Duration = Duration.ofSeconds(5)
	override val buildUp: Duration = Duration.ofMillis(500)
	override val maxWait: Duration = Duration.ofSeconds(120)
}

data class Limited(
	/**
	 * Maximum amount of attempts that will be made before giving up an job
	 * when it yields a [ConsumerResult.TransitoryFailure]
	 */
	val attempts: Long,
	override val initialBackOff: Duration = Duration.ofSeconds(10),
	override val buildUp: Duration = Duration.ofSeconds(1),
	override val maxWait: Duration = Duration.ofSeconds(60)
): RetryPolicy() {
	init {
		require(attempts > 0)
		require(!initialBackOff.isNegative)
		require(!buildUp.isNegative)
		require(!maxWait.isNegative)
		require(initialBackOff < maxWait)
	}
}
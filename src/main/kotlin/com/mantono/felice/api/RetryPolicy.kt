package com.mantono.felice.api

import java.time.Duration

interface RetryPolicy {
	/**
	 * Initial delay between each subsequent attempts when making a retry.
	 * Cannot be negative.
	 */
	val initialBackOff: Duration
	/**
	 * The increase of the delay that is added on top of the initial delay from
	 * [initialBackOff] for each time a job fails with a [ConsumerResult.TransitoryFailure]
	 * Cannot be negative.
	 */
	val buildUp: Duration
	/**
	 * The maximum amount of wait time that can be accumulated from [initialBackOff]
	 * plus [buildUp] time
	 * Cannot be negative.
	 */
	val maxWait: Duration

	fun waitTime(attemptsDone: Long): Duration {
		require(attemptsDone >= 0)
		val accumulatedBuildUp = buildUp.multipliedBy(attemptsDone)
		return initialBackOff.plus(accumulatedBuildUp).coerceAtMost(maxWait)
	}

	fun shouldRetry(result: MessageResult): Boolean {
		val isNotTransitory: Boolean = result.result !is ConsumerResult.TransitoryFailure
		val hasUsedAllAttempts: Boolean = this is Limited && this.attempts <= result.attempts
		return when {
			isNotTransitory -> false
			hasUsedAllAttempts -> false
			else -> true
		}
	}
}

object Infinite: RetryPolicy {
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
): RetryPolicy {
	init {
		require(attempts > 0)
		require(!initialBackOff.isNegative)
		require(!buildUp.isNegative)
		require(!maxWait.isNegative)
		require(initialBackOff < maxWait)
	}
}
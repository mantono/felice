package com.mantono.felice.api

sealed class ConsumerResult {
	object Succes: ConsumerResult()
	data class Retry(val message: String? = null): ConsumerResult()
	data class PermanentFailure(val message: String? = null): ConsumerResult()
}

fun Throwable.asRetry(): ConsumerResult.Retry = ConsumerResult.Retry(this.message)
fun Throwable.asFailure(): ConsumerResult.PermanentFailure = ConsumerResult.PermanentFailure(this.message)

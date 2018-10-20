package com.mantono.felice.api

sealed class ConsumerResult<out T> {
	data class Succes<out T>(val value: T): ConsumerResult<T>()
	data class Retry<out T>(val exception: Throwable? = null): ConsumerResult<T>()
	data class PermanentFailure<out T>(val message: String? = null): ConsumerResult<T>()
}

fun <T> Throwable.asRetry(): ConsumerResult.Retry<T> = ConsumerResult.Retry(this)
fun <T> Throwable.asFailure(): ConsumerResult.PermanentFailure<T> = ConsumerResult.PermanentFailure(this.message)

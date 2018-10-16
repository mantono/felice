package com.mantono.felice.api

sealed class ConsumeResult {
	object Success: ConsumeResult()
	data class Retry(val exception: Throwable? = null)
	data class PermanentFailure(val message: String? = null)
}

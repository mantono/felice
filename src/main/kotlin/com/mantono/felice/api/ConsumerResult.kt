package com.mantono.felice.api

/**
 * Defines the outcome of a processed job
 */
sealed class ConsumerResult {
	/**
	 * The job was successful
	 */
	object Success: ConsumerResult()

	/**
	 * The job failed.
	 * The failure is considered to be transient, and a different outcome may be
	 * expected if the job is processed in the future. This is usually the case for
	 * network related errors where some resource is temporarily unavailable but may
	 * become available at a later time.
	 */
	data class TransitoryFailure(
		/**
		 * Optional error message that may tell why the job failed
		 */
		val message: String? = null
	): ConsumerResult()

	/**
	 * The job failed.
	 * The failure is of permanent nature and it is not worth making a new attempts
	 * at processing the job. This will apply to many different types of errors, but
	 * most scenarios will be where inputs will not change and the outcome is not
	 * dependent on or will not change because of any outside factors such as network
	 * activities or available disk space. Examples that warrants a [PermanentFailure] failure
	 * may be parsing errors or arithmetic errors that will always yield the same result
	 * given the jobs input.
	 */
	data class PermanentFailure(
		/**
		 * Optional error message that may tell why the job failed
		 */
		val message: String? = null
	): ConsumerResult()

	override fun toString(): String = when(this) {
		Success -> "Success"
		is PermanentFailure -> "PermanentFailure${message?.let { ": $it" }}"
		is TransitoryFailure -> "TransitoryFailure${message?.let { ": $it" }}"
	}
}

fun Throwable.asTransitory(): ConsumerResult.TransitoryFailure = ConsumerResult.TransitoryFailure(this.message)
fun Throwable.asPermanent(): ConsumerResult.PermanentFailure = ConsumerResult.PermanentFailure(this.message)
package com.mantono.felice.api

interface Interceptor {
	/**
	 * Retrieve a message before it is sent to the MessageConsumer, read it and
	 * if necessary, modify it before it is forwarded in the pipeline.
	 */
	fun <K, V> intercept(message: Message<K, V>): Message<K, V> = message

	/**
	 * Function to execute when the MessageConsumer returns a ConsumerResult.
	 */
	fun onResult(result: ConsumerResult) {}

	/**
	 * Function to execute when the MessageConsumer throws an exception that is
	 * not caught within the MessageConsumer.
	 */
	fun onException(exception: Throwable) {}
}

fun <K, V> List<Interceptor>.foldMessage(message: Message<K, V>): Message<K, V> {
	return this.fold(message) { msg: Message<K, V>, interceptor: Interceptor ->
		interceptor.intercept(msg)
	}
}
package com.mantono.felice.api.worker

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.Message

interface Pipeline<K, V> {
	/**
	 * Retrieve a message before it is sent to the MessageConsumer, read it and
	 * if neccesseary, modify it before it is forwarded in the pipeline.
	 */
	fun intercept(message: Message<K, V>): Message<K, V> = message

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
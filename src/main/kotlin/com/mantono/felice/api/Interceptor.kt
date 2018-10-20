package com.mantono.felice.api

interface Interceptor<K, V> {
	fun intercept(message: Message<K, V>): Unit
}
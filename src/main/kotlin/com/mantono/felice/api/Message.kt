package com.mantono.felice.api

interface Message<K, V> {
	val topic: String
	val headers: Map<String, Any>
	val key: K?
	val value: V?
	val offset: Long
	val partition: Int
}
package com.mantono.felice.implementation

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

fun dispatcher(threads: UInt): CoroutineDispatcher {
	val coreSize: Int = (threads.toInt() / 4).coerceAtLeast(1)
	val maxSize: Int = threads.toInt()
	val threadPool = ThreadPoolExecutor(coreSize, maxSize, 120L, TimeUnit.SECONDS, ArrayBlockingQueue(maxSize*100))
	return threadPool.asCoroutineDispatcher()
}
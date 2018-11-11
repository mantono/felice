package com.mantono.felice.implementation

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

class WorkerScope(threads: UInt): CoroutineScope {
	override val coroutineContext: CoroutineContext = dispatcher(threads)
}

fun dispatcher(threads: UInt = 2u): CoroutineDispatcher {
	val coreSize: Int = (threads.toInt() / 4).coerceAtLeast(2)
	val maxSize: Int = threads.toInt().coerceAtLeast(coreSize)
	val threadPool = ThreadPoolExecutor(coreSize, maxSize, 120L, TimeUnit.SECONDS, ArrayBlockingQueue(maxSize*100))
	return threadPool.asCoroutineDispatcher()
}
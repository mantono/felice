package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.concurrent.Semaphore

private val log = KotlinLogging.logger("work-distributor")

class WorkDistributor<K, V>(
	private val worker: Worker<K, V>,
	private val work: Director<K, V>
) {
	private val allowStart = Semaphore(1)

	fun start(scope: CoroutineScope = GlobalScope): Boolean {
		return allowStart.tryAcquire().onTrue {
			scope.launch { work(scope) }
		}
	}

	private tailrec suspend fun work(scope: CoroutineScope) {
		if(!scope.isActive) {
			return
		}

		log.debug { "Polling..." }
		val message: Message<K, V> = work.receive()
		launchConsumer(scope, worker, work, message)

		work(scope)
	}
}
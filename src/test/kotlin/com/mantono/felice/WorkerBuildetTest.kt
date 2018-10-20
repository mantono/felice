package com.mantono.felice

import com.mantono.felice.api.ConsumerResult
import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.Message
import com.mantono.felice.api.FeliceWorkerBuilder
import com.mantono.felice.api.WorkerBuilder
import com.mantono.felice.api.worker.Worker
import kotlinx.coroutines.Job
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WorkerBuildetTest {

	@Test
	fun testNormalControlFlow() {

		val worker: Worker<String, String> = WorkerBuilder<String, String>()
			.topic("topic1", "topic2")
			.groupId("my-groupId")
			.options(emptyMap())
			.deserializeKey { String(it!!) }
			.deserializeValue { String(it!!) }
			.intercept { println(it) }
			.pipe { it.copy(value = it.value!!.toUpperCase()) }
			.consumer {
				println("${it.topic} / ${it.partition} / ${it.offset}")
				ConsumerResult.Succes
			}
			.build()

		assertTrue(job.isActive)
		job.cancel()
		assertTrue(job.isCancelled)

	}
}
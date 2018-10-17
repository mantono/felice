package com.mantono.felice

import com.mantono.felice.api.ConsumeResult
import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.Message
import com.mantono.felice.implementation.FeliceWorkerBuilder
import kotlinx.coroutines.Job
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WorkerBuildetTest {

	@Test
	fun testNormalControlFlow() {
		val consumer = object: MessageConsumer<String, String> {
			override suspend fun consume(message: Message<String, String>): ConsumeResult =
					ConsumeResult.Success
		}

		val job: Job = FeliceWorkerBuilder<String, String>()
			.topic("topic1", "topic2")
			.groupId("my-groupId")
			.options(emptyMap())
			.consumer(consumer)
			.start()

		assertTrue(job.isActive)
		job.cancel()
		assertTrue(job.isCancelled)

	}
}
package com.mantono.felice.api.worker

import com.mantono.felice.api.MessageConsumer
import com.mantono.felice.api.Interceptor
import com.mantono.felice.implementation.execute
import kotlinx.coroutines.Job
import java.util.concurrent.atomic.AtomicReference

class FeliceWorker<K, V>(
	override val groupId: String,
	override val topics: Set<String>,
	override val options: Map<String, Any>,
	override val interceptors: List<Interceptor<K, V>>,
	override val consumer: MessageConsumer<K, V>
): Worker<K, V>
{
	override fun deserializeKey(bytes: ByteArray): K
	{
		TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
	}

	override fun deserializeValue(bytes: ByteArray): V
	{
		TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
	}

	private val job: AtomicReference<Job> = AtomicReference()


}

fun <K, V> Worker<K, V>.start(): Job = job.updateAndGet { current: Job? ->
	current ?: execute(this)
}
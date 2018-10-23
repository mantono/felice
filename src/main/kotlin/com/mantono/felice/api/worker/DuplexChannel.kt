package com.mantono.felice.api.worker

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import java.util.concurrent.Semaphore

interface DuplexChannel<out I, in O>: ReceiveChannel<I>, SendChannel<O>

class TransactionChannel<out I, in O> private constructor(
	private val inbound: ReceiveChannel<I> = Channel(1),
	private val outbound: SendChannel<O> = Channel(1)
): DuplexChannel<I, O>, ReceiveChannel<I> by inbound, SendChannel<O> by outbound {
	private val transactionTicker = Semaphore(1)
}
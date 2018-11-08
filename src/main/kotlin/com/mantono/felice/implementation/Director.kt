package com.mantono.felice.implementation

import com.mantono.felice.api.Message
import com.mantono.felice.api.MessageResult
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel

interface Director<K, V> : ReceiveChannel<Message<K, V>>, SendChannel<MessageResult<K, V>> {
	fun start(scope: CoroutineScope = GlobalScope): Boolean
}
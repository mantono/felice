package com.mantono.felice.implementation.actors

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel

interface ActorScope<T>: CoroutineScope, Channel<T>, Actor<T>
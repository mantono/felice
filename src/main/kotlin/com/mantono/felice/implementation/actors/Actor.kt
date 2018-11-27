package com.mantono.felice.implementation.actors

import kotlinx.coroutines.channels.SendChannel

interface Actor<T>: SendChannel<T> {
	fun isActive(): Boolean
	fun stop(): Boolean
}
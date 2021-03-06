package com.mantono.felice.api

import org.apache.kafka.common.TopicPartition

data class MessageResult(
	val result: ConsumerResult,
	val topicPartition: TopicPartition,
	val offset: Long,
	val attempts: Long
)
package com.mmakowski.scratch.logrep.common

import java.io.File

import kafka.common.TopicAndPartition
import kafka.log.{CleanerConfig, Log, LogConfig, LogManager}
import kafka.server.{BrokerState, FetchDataInfo}
import kafka.utils.{KafkaScheduler, SystemTime}

final class KafkaLog(logDir: File) {
  private val topicName = "logrep-topic"
  private val tap = TopicAndPartition(topicName, 0)
  private val topicConfig = LogConfig()
  private val scheduler = new KafkaScheduler(threads = 5, threadNamePrefix = "log-scheduler-", daemon = true)
  private val logManager = new LogManager(logDirs = Array(logDir),
                                          topicConfigs = Map(topicName -> topicConfig),
                                          defaultConfig = topicConfig,
                                          cleanerConfig = CleanerConfig(),
                                          ioThreads = 5,
                                          flushCheckMs = 10000,
                                          flushCheckpointMs = 10000,
                                          retentionCheckMs = 10000,
                                          scheduler = scheduler,
                                          brokerState = BrokerState(),
                                          time = SystemTime)

  def startup(): Unit = {
    scheduler.startup()
    logManager.startup()
  }

  def shutdown(): Unit = {
    logManager.shutdown()
    scheduler.shutdown()
  }

  lazy val log: Log = logManager.getLog(tap).getOrElse(logManager.createLog(tap, topicConfig))
}

final class KafkaLogReader(log: Log) {
  def read(startOffset: Long, maxLengthBytes: Int, maxOffset: Option[Long] = None): FetchDataInfo =
    log.read(startOffset, maxLengthBytes, maxOffset)
}
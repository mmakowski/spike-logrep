package com.mmakowski.scratch.logrep

import java.io.File
import java.nio.file.Files

import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.server.BrokerState
import kafka.utils.{SystemTime, KafkaScheduler}

object Leader {
  def main(args: Array[String]): Unit = {
    val logDir = Files.createTempDirectory("logrep-")
    val topicName = "logrep-topic"
    val topicConfig = LogConfig()
    println(s"log dir: $logDir")

    val scheduler = new KafkaScheduler(threads = 5, threadNamePrefix = "log-scheduler-", daemon = true)
    val logManager = new LogManager(logDirs = Array(logDir.toFile),
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

    scheduler.startup()
    logManager.startup()

    try produce(logManager)
    finally {
      logManager.shutdown()
      scheduler.shutdown()
    }
  }

  private def produce(logManager: LogManager): Unit = ()
}

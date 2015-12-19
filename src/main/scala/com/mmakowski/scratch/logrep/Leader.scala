package com.mmakowski.scratch.logrep

import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import kafka.common.TopicAndPartition
import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.message.{Message, NoCompressionCodec, ByteBufferMessageSet}
import kafka.server.BrokerState
import kafka.utils.{KafkaScheduler, SystemTime}
import org.slf4j.LoggerFactory

import scala.util.Random

object Leader {
  val logger = LoggerFactory.getLogger(this.getClass)

  val topicName = "logrep-topic"
  val topicConfig = LogConfig()
  val batchSizes = Seq(1, 100, 10000, 1000000) //, 100000000)

  def main(args: Array[String]): Unit = {
    val logDir = Files.createTempDirectory("logrep-")
    logger.info("log dir: {}", logDir)

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

  private def produce(logManager: LogManager): Unit = {
    val tap = TopicAndPartition(topicName, 0)
    val log = logManager.getLog(tap).getOrElse(logManager.createLog(tap, topicConfig))
    val offset = new AtomicLong(0L)

    while (true) {
      val nextBatch = createBatch(offset)
      logger.debug("appending...")
      log.append(nextBatch, assignOffsets = false)
      logger.debug("appended")
    }
  }

  private def createBatch(offset: AtomicLong): ByteBufferMessageSet = {
    val batchSize = batchSizes(Random.nextInt(batchSizes.length))
    logger.debug("generating {} messages from offset {}...", batchSize, offset.get)
    val messagesSeq = messages(offset.get, batchSize)
    new ByteBufferMessageSet(NoCompressionCodec, offset, messagesSeq: _*)
  }

  private def messages(startPayload: Long, count: Int): Seq[Message] =
    startPayload.until(startPayload + count).map { payload =>
      val buf = ByteBuffer.allocate(8)
      buf.putLong(payload)
      require(buf.array.length == 8, "unexpected array size " + buf.array.length)
      new Message(buf.array)
    }
}

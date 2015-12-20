package com.mmakowski.scratch.logrep.leader

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import com.mmakowski.scratch.logrep.common.{KafkaLogReader, KafkaLog}
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import org.slf4j.LoggerFactory

import scala.util.Random

object Leader {
  val logger = LoggerFactory.getLogger(this.getClass)

  val batchSizes = Seq(        1,         1,         1,         1,         1,
                             100,       100,       100,       100,       100,       100,       100,
                           10000,     10000,     10000,     10000,     10000,     10000,
                         1000000,   1000000,
                        25000000)

  def main(args: Array[String]): Unit = {
    val logDir = new File(args(0))
    logger.info("log dir: {}", logDir)

    val kafka = new KafkaLog(logDir)
    kafka.startup()

    val repServer = new ReplicationServer(12321, new KafkaLogReader(kafka.log))
    logger.info("starting rep server...")
    repServer.startup()
    logger.info("rep server started")

    Thread.sleep(Long.MaxValue) // for testing rep server only

    try produce(kafka)
    finally kafka.shutdown()
  }

  private def produce(kafka: KafkaLog): Unit = {
    val offset = new AtomicLong(kafka.log.nextOffsetMetadata.messageOffset)
    logger.debug("starting from offset {}", offset.get)

    while (true) {
      val nextBatch = createBatch(offset)
      logger.debug("appending...")
      kafka.log.append(nextBatch, assignOffsets = false)
      kafka.log.flush()
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

package com.mmakowski.scratch.logrep.validator

import java.io.File

import com.mmakowski.scratch.logrep.common.{KafkaLogReader, KafkaLog}
import kafka.message.MessageAndOffset
import org.slf4j.LoggerFactory

object LogValidator {
  val logger = LoggerFactory.getLogger(this.getClass)
  val maxBytes = 100 * 1024 * 1024

  def main(args: Array[String]): Unit = {
    val logDir = new File(args(0))
    logger.info("log dir: {}", logDir)

    val kafka = new KafkaLog(logDir)
    kafka.startup()
    try verify(new KafkaLogReader(kafka.log))
    finally kafka.shutdown()
  }

  private def verify(reader: KafkaLogReader): Unit = {
    var offset = 0L
    var endReached = false
    while (!endReached) {
      logger.info("validating from offset {}", offset)
      val fetch = reader.read(offset, maxBytes)
      if (fetch.messageSet.isEmpty) {
        if (fetch.fetchOffsetMetadata.messageOffset == offset) endReached = true
        else offset = fetch.fetchOffsetMetadata.messageOffset
      } else {
        fetch.messageSet.foreach { mo =>
          assert(mo.nextOffset == offset + 1, s"expected next offset to be ${offset + 1} but got ${mo.nextOffset}")
          verify(mo)
          offset = mo.nextOffset
        }
      }
    }
    logger.info("log validated until offset {}", offset)
  }

  private def verify(mo: MessageAndOffset): Unit = {
    val payload = mo.message.payload.getLong()
    assert(payload == mo.offset, s"got payload $payload at offset ${mo.offset}")
  }
}

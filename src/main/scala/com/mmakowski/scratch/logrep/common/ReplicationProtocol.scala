package com.mmakowski.scratch.logrep.common

import io.netty.buffer.{ByteBufAllocator, ByteBuf}

object ReplicationProtocol {
  sealed trait Message {
    def toByteBuf(alloc: ByteBufAllocator): ByteBuf
  }

  final case class Start(fromOffset: Long) extends Message {
    def toByteBuf(alloc: ByteBufAllocator): ByteBuf = {
      val buf = alloc.buffer(2 + 8)
      buf.writeShort(Start.Id)
      buf.writeLong(fromOffset)
    }
  }
  object Start {
    val Id = 0.toShort

    def parse(buf: ByteBuf): Start = ensuringReadable(buf) {
      Start(buf.readLong())
    }
  }

  def parse(buf: ByteBuf): Message = ensuringReadable(buf) {
    val message = buf.readShort match {
      case Start.Id => Start.parse(buf)
      case unrecognised => sys.error(s"unrecognised message type id: $unrecognised")
    }
    require(!buf.isReadable, "data remaining in buffer")
    message
  }

  private def ensuringReadable[T](buf: ByteBuf)(body: => T): T = {
    require(buf.isReadable, "buffer is not readable")
    body
  }
}

package com.mmakowski.scratch.logrep.common

import java.nio.ByteBuffer

import com.mmakowski.scratch.logrep.common.ReplicationProtocol.Message
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToByteEncoder}

object ReplicationProtocol {
  sealed trait Message {
    def write(buf: ByteBuf): Unit
  }

  sealed trait ParseResult
  object ParseResult {
    final case class Success(message: Message) extends ParseResult
    final case class UnrecognisedMessageId(id: Short) extends ParseResult
    object InsufficientBytes extends ParseResult
  }

  final case class Subscribe(fromOffset: Long) extends Message {
    def write(buf: ByteBuf): Unit = {
      buf.writeShort(Subscribe.Id)
      buf.writeLong(fromOffset)
    }
  }
  object Subscribe {
    val Id = 0.toShort

    def parse(buf: ByteBuf): ParseResult =
      if (buf.readableBytes < 8) ParseResult.InsufficientBytes
      else ParseResult.Success(Subscribe(buf.readLong()))
  }

  final case class LogEntries(entryBytes: ByteBuffer) extends Message {
    def write(buf: ByteBuf): Unit = {
      buf.writeShort(LogEntries.Id)
      buf.writeLong(entryBytes.limit)
      buf.writeBytes(entryBytes)
    }
  }
  object LogEntries {
    val Id = 1.toShort

    def parse(buf: ByteBuf): ParseResult =
      if (buf.readableBytes < 8) ParseResult.InsufficientBytes
      else {
        val length = buf.readLong()
        if (buf.readableBytes < length) ParseResult.InsufficientBytes
        else {
          val entryBytes = ByteBuffer.allocate(length.toInt)
          buf.readBytes(entryBytes)
          ParseResult.Success(LogEntries(entryBytes))
        }
      }
  }

  def parse(buf: ByteBuf): ParseResult =
    if (buf.readableBytes < 2) ParseResult.InsufficientBytes
    else
      buf.readShort match {
        case Subscribe.Id => Subscribe.parse(buf)
        case LogEntries.Id => LogEntries.parse(buf)
        case unrecognised => ParseResult.UnrecognisedMessageId(unrecognised)
      }
}

final class MessageDecoder extends ByteToMessageDecoder {
  import ReplicationProtocol.ParseResult._

  def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: java.util.List[AnyRef]): Unit =
    ReplicationProtocol.parse(in) match {
      case Success(msg) => out.add(msg)
      case InsufficientBytes => in.resetReaderIndex()
      case UnrecognisedMessageId(id) => sys.error(s"unrecognised message id: $id")
    }
}

final class MessageEncoder extends MessageToByteEncoder[ReplicationProtocol.Message] {
  def encode(ctx: ChannelHandlerContext, msg: Message, out: ByteBuf): Unit = msg.write(out)
}

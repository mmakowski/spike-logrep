package com.mmakowski.scratch.logrep.common

import io.netty.buffer.{ByteBuf, ByteBufAllocator}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

object ReplicationProtocol {
  sealed trait Message {
    def toByteBuf(alloc: ByteBufAllocator): ByteBuf
  }

  sealed trait ParseResult
  object ParseResult {
    final case class Success(message: Message) extends ParseResult
    final case class UnrecognisedMessageId(id: Short) extends ParseResult
    object InsufficientBytes extends ParseResult
  }

  final case class Subscribe(fromOffset: Long) extends Message {
    def toByteBuf(alloc: ByteBufAllocator): ByteBuf = {
      val buf = alloc.buffer(2 + 8)
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

  def parse(buf: ByteBuf): ParseResult =
    if (buf.readableBytes < 2) ParseResult.InsufficientBytes
    else
      buf.readShort match {
        case Subscribe.Id => Subscribe.parse(buf)
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

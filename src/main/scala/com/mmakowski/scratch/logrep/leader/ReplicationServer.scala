package com.mmakowski.scratch.logrep.leader

import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel

import com.mmakowski.scratch.logrep.common.{KafkaLogReader, MessageDecoder, MessageEncoder, ReplicationProtocol}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import kafka.message.MessageSet
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

private[leader] class ReplicationServer(port: Int, logReader: KafkaLogReader) {
  def startup(): Unit = {
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    val b = new ServerBootstrap()
    b.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        def initChannel(ch: SocketChannel): Unit = ch.pipeline.addLast(
          new MessageDecoder,
          new MessageEncoder,
          new ReplicationServerHandler(logReader))
      })
      .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
      .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

    b.bind(port).sync()
  }
}

private final class ReplicationServerHandler(logReader: KafkaLogReader) extends SimpleChannelInboundHandler[ReplicationProtocol.Message] {
  private val logger = LoggerFactory.getLogger(this.getClass)
  // TODO: messages of this size don't work, cause infinite looping in the client. We should probably use ChunkedWriteHandler instead
  // it could even make the protocol simpler
  val MaxBytes = 1024 //* 1024

  override def channelRead0(ctx: ChannelHandlerContext, protocolMessage: ReplicationProtocol.Message): Unit = {
    protocolMessage match {
      case ReplicationProtocol.Subscribe(startOffset) => publish(ctx, startOffset)
      case _                                          => sys.error(s"unexpected message: $protocolMessage")
    }
  }

  private def publish(ctx: ChannelHandlerContext, startOffset: Long): Unit = {
    logger.info("publishing from offset {}", startOffset)
    val fetch = logReader.read(startOffset, MaxBytes)
    if (fetch.messageSet.nonEmpty) {
      val nextOffset = fetch.messageSet.last.nextOffset
      val write = ctx.writeAndFlush(ReplicationProtocol.LogEntries(toByteBuffer(fetch.messageSet)))
      write.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture): Unit = {
          assert(future == write)
          publish(ctx, nextOffset)
        }
      })
    } else {
//      logger.info("no messages read, sleeping...")
//      Thread.sleep(10000)
//      publish(ctx, startOffset)
    }
  }

  private def toByteBuffer(messageSet: MessageSet): ByteBuffer = {
    val channel = new ByteBufferChannel(messageSet.sizeInBytes)
    messageSet.writeTo(channel, 0, messageSet.sizeInBytes)
    channel.buffer
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

private final class ByteBufferChannel(size: Int) extends GatheringByteChannel {
  val buffer = ByteBuffer.allocate(size)

  def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long = ???

  def write(srcs: Array[ByteBuffer]): Long = ???

  def write(src: ByteBuffer): Int = {
    // TODO: this copying should not be necessary
    buffer.put(src)
    buffer.flip()
    src.limit
  }

  def isOpen: Boolean = true

  def close(): Unit = ()
}
package com.mmakowski.scratch.logrep.leader

import com.mmakowski.scratch.logrep.log.KafkaLogReader
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel._

private[leader] class ReplicationServer(port: Int, logReader: KafkaLogReader) {
  def startup(): Unit = {
    val bossGroup = new NioEventLoopGroup()
    val workerGroup = new NioEventLoopGroup()
    val b = new ServerBootstrap()
    b.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel]() {
        def initChannel(ch: SocketChannel): Unit = ch.pipeline().addLast(new ReplicationHandler(logReader))
      })
      .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
      .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

    b.bind(port).sync()
  }
}

private final class ReplicationHandler(logReader: KafkaLogReader) extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    println(msg)
    msg.asInstanceOf[ByteBuf].release()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}

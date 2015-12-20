package com.mmakowski.scratch.logrep.follower

import com.mmakowski.scratch.logrep.common.ReplicationProtocol
import com.mmakowski.scratch.logrep.common.ReplicationProtocol.Start
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.util.ReferenceCountUtil

object Follower {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 12321

    val workerGroup = new NioEventLoopGroup
    val b = new Bootstrap
    b.group(workerGroup)
     .channel(classOf[NioSocketChannel])
     .option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
     .handler(new ChannelInitializer[SocketChannel]() {
       def initChannel(ch: SocketChannel): Unit = ch.pipeline.addLast(new ReplicationClientHandler)
     })
    val f = b.connect(host, port).sync()
    f.channel.closeFuture.sync()
  }
}

final class ReplicationClientHandler extends ChannelInboundHandlerAdapter {
  override def channelActive(ctx: ChannelHandlerContext): Unit =
    ctx.writeAndFlush(Start(0).toByteBuf(ctx.alloc))

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit =
    try {
      val buf = msg.asInstanceOf[ByteBuf]
      val protocolMessage = ReplicationProtocol.parse(buf)
      println(protocolMessage)
      // TODO: start serving log
    } finally ReferenceCountUtil.release(msg)

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
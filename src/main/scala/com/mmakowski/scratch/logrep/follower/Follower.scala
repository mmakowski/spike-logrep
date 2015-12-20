package com.mmakowski.scratch.logrep.follower

import com.mmakowski.scratch.logrep.common.ReplicationProtocol.Subscribe
import com.mmakowski.scratch.logrep.common.{MessageDecoder, ReplicationProtocol}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption}

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
       def initChannel(ch: SocketChannel): Unit = ch.pipeline.addLast(new MessageDecoder, new ReplicationClientHandler)
     })
    val f = b.connect(host, port).sync()
    f.channel.closeFuture.sync()
  }
}

final class ReplicationClientHandler extends ChannelInboundHandlerAdapter {
  override def channelActive(ctx: ChannelHandlerContext): Unit =
    ctx.writeAndFlush(Subscribe(0).toByteBuf(ctx.alloc))

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    val protocolMessage = msg.asInstanceOf[ReplicationProtocol.Message]
    println(protocolMessage)
    // TODO: act on the message
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
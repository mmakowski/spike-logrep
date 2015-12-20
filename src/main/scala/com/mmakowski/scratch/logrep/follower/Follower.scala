package com.mmakowski.scratch.logrep.follower

import com.mmakowski.scratch.logrep.common.ReplicationProtocol.Subscribe
import com.mmakowski.scratch.logrep.common.{MessageEncoder, MessageDecoder, ReplicationProtocol}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel._

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
       def initChannel(ch: SocketChannel): Unit = ch.pipeline.addLast(
         new MessageDecoder,
         new MessageEncoder,
         new ReplicationClientHandler)
     })
    val f = b.connect(host, port).sync()
    f.channel.closeFuture.sync()
  }
}

final class ReplicationClientHandler extends SimpleChannelInboundHandler[ReplicationProtocol.Message] {
  override def channelActive(ctx: ChannelHandlerContext): Unit =
    ctx.writeAndFlush(Subscribe(0))

  override def channelRead0(ctx: ChannelHandlerContext, protocolMessage: ReplicationProtocol.Message): Unit = {
    println(protocolMessage)
    // TODO: act on the message
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
package com.wiqer.redis;


import com.wiqer.redis.aof.Aof;
import com.wiqer.redis.channel.LocalChannelOption;
import com.wiqer.redis.channel.single.SingleSelectChannelOption;
import com.wiqer.redis.netty.channel.socket.NioSingleServerSocketChannel;
import com.wiqer.redis.util.PropertiesUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.log4j.Logger;


import java.net.InetSocketAddress;

public class MyRedisServer implements RedisServer {
    private static final Logger LOGGER = Logger.getLogger(MyRedisServer.class);
    private final RedisCore redisCore = new RedisCoreImpl();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final EventExecutorGroup redisSingleEventExecutor;
    private final LocalChannelOption<NioSingleServerSocketChannel> channelOption;
    private Aof aof;

    public MyRedisServer(LocalChannelOption<NioSingleServerSocketChannel> channelOption) {
        this.channelOption = channelOption;
        this.redisSingleEventExecutor = new NioEventLoopGroup(1);
    }

    public static void main(String[] args) {
        new MyRedisServer(new SingleSelectChannelOption()).start();
    }

    @Override
    public void start() {
        if (PropertiesUtil.getAppendOnly()) {
            aof = new Aof(this.redisCore);
        }
        start0();
    }

    @Override
    public void close() {
        try {
            channelOption.boss().shutdownGracefully();
            channelOption.selectors().shutdownGracefully();
            redisSingleEventExecutor.shutdownGracefully();
        } catch (Throwable t) {
            LOGGER.warn("Exception!", t);
        }
    }

    public void start0() {
        serverBootstrap.group(channelOption.boss(), channelOption.selectors())
                .channel(channelOption.getChannelClass())
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, PropertiesUtil.getTcpKeepAlive())
                .localAddress(new InetSocketAddress(PropertiesUtil.getNodeAddress(), PropertiesUtil.getNodePort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel)  {
                        ChannelPipeline channelPipeline = socketChannel.pipeline();
                        channelPipeline.addLast(
                                new ResponseEncoder(),
                                new CommandDecoder(aof)
                        );
                        channelPipeline.addLast(redisSingleEventExecutor, new CommandHandler(redisCore));
                    }
                });

        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            LOGGER.info(sync.channel().localAddress().toString());
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public RedisCore getRedisCore() {
        return redisCore;
    }
}

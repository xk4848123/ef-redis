package com.wiqer.redis.channel.single;

import com.wiqer.redis.channel.LocalChannelOption;
import com.wiqer.redis.netty.channel.nio.NioSingleEventLoopGroup;
import com.wiqer.redis.netty.channel.socket.NioSingleServerSocketChannel;
import io.netty.channel.EventLoopGroup;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleSelectChannelOption implements LocalChannelOption<NioSingleServerSocketChannel> {
    private final NioSingleEventLoopGroup single;

    public SingleSelectChannelOption(NioSingleEventLoopGroup single) {
        this.single = single;
    }

    public SingleSelectChannelOption()
    {
        this.single = new NioSingleEventLoopGroup( new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(@NotNull Runnable r) {
                return new Thread(r, "Server_boss_" + index.getAndIncrement());
            }
        });

    }
    @Override
    public EventLoopGroup boss() {
        return  this.single;
    }

    @Override
    public EventLoopGroup selectors() {
        return  this.single;
    }

    @Override
    public Class<NioSingleServerSocketChannel> getChannelClass() {
        return NioSingleServerSocketChannel.class;
    }
}

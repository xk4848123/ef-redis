package com.wiqer.redis.channel;


import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

public interface LocalChannelOption<C extends Channel>  {

    EventLoopGroup boss();

    EventLoopGroup selectors();

    Class<? extends C> getChannelClass();
}

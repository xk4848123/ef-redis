package com.wiqer.redis.command;


import com.wiqer.redis.RedisCore;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;


import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface Command {

    Charset CHARSET = StandardCharsets.UTF_8;

    org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Command.class);

    CommandType type();

    void setContent(Resp[] array);

    void handle(ChannelHandlerContext ctx, RedisCore redisCore);
}

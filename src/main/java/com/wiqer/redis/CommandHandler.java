package com.wiqer.redis;

import com.wiqer.redis.command.Command;
import com.wiqer.redis.util.TRACEID;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandHandler.class);

    private final RedisCore redisCore;

    public CommandHandler(RedisCore redisCore) {
        this.redisCore = redisCore;
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
        String traceId = TRACEID.currentTraceId();
        LOGGER.debug("traceId:" + traceId + " 本次处理的命令：" + command.type().name());
        try {
            command.handle(ctx, redisCore);
        } catch (Throwable t) {
            LOGGER.error("处理数据时", t);
        }
        LOGGER.debug("traceId:" + traceId + " 命令处理完毕");
    }

}

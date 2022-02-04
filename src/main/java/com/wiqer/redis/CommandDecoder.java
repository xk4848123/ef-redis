package com.wiqer.redis;

import com.wiqer.redis.aof.Aof;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommandFactory;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.resp.*;
import com.wiqer.redis.util.TRACEID;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.log4j.Logger;


public class CommandDecoder extends LengthFieldBasedFrameDecoder {

    private static final Logger LOGGER = Logger.getLogger(CommandDecoder.class);

    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;

    private Aof aof = null;

    public CommandDecoder(Aof aof) {
        this();
        this.aof = aof;
    }

    public CommandDecoder() {
        super(MAX_FRAME_LENGTH, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        TRACEID.newTraceId();
        try {
            Resp resp = Resp.decode(in);
            if (!(resp instanceof RespArray)) {
                throw new IllegalStateException("客户端发送的命令格式有问题");
            }
            Command command;
            command = CommandFactory.from((RespArray) resp);

            if (command == null) {
                ctx.writeAndFlush(new Errors("unsupport command:" + ((BulkString) ((RespArray) resp).getArray()[0]).getContent().toUtf8String()));
            } else {
                if (aof != null && command instanceof WriteCommand) {
                    aof.put(resp);
                }
                return command;
            }
        } catch (Throwable t) {
            LOGGER.error("解码命令", t);
        }
        return null;
    }


}

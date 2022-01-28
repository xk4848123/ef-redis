package com.wiqer.redis;


import com.wiqer.redis.resp.Resp;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseEncoder extends MessageToByteEncoder<Resp> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Resp resp, ByteBuf byteBuf) throws Exception {
        try {
            Resp.write(resp, byteBuf);
            byteBuf.writeBytes(byteBuf);
        } catch (Throwable t) {
            LOGGER.error("response error: ", t);
        }
    }

}

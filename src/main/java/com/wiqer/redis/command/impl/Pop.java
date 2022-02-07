package com.wiqer.redis.command.impl;

import com.wiqer.redis.RedisCore;
import com.wiqer.redis.command.CommandType;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.datatype.BytesWrapper;
import com.wiqer.redis.datatype.RedisData;
import com.wiqer.redis.datatype.RedisList;
import com.wiqer.redis.resp.BulkString;
import com.wiqer.redis.resp.Errors;
import com.wiqer.redis.resp.Resp;
import io.netty.channel.ChannelHandlerContext;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;

public abstract class Pop implements WriteCommand {

    private BytesWrapper key;

    @Override
    public void setContent(Resp[] array) {
        key = ((BulkString) array[1]).getContent();
    }

    @Override
    public void handle(ChannelHandlerContext ctx, RedisCore redisCore) {
        RedisData redisData = redisCore.get(key);
        if (redisData == null) {
            ctx.writeAndFlush(BulkString.NullBulkString);
        } else if (!(redisData instanceof RedisList)) {
            ctx.writeAndFlush(new Errors("wrong type"));
        } else {
            BytesWrapper bytesWrapper = popBytesWrapper(redisCore, (RedisList) redisData);
            ctx.writeAndFlush(new BulkString(bytesWrapper));
        }
    }

    @Override
    public void handle(RedisCore redisCore) {
        RedisData redisData = redisCore.get(key);
        if (redisData instanceof RedisList) {
            popBytesWrapper(redisCore, (RedisList) redisData);
        }
    }

    @Nullable
    private BytesWrapper popBytesWrapper(RedisCore redisCore, RedisList redisData) {
        BytesWrapper bytesWrapper = null;
        if (type() == CommandType.rpop) {
            bytesWrapper = redisData.rpop();
        } else if (type() == CommandType.lpop) {
            bytesWrapper = redisData.lpop();
        }
        if (redisData.size() == 0) {
            redisCore.remove(Collections.singletonList(key));
        }
        return bytesWrapper;
    }
}

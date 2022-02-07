package com.wiqer.redis.command.impl.list;

import com.wiqer.redis.command.CommandType;
import com.wiqer.redis.command.impl.Push;
import com.wiqer.redis.datatype.RedisList;

public class Rpush extends Push {

    public Rpush() {
        super(RedisList::rpush);
    }

    @Override
    public CommandType type() {
        return CommandType.rpush;
    }
}

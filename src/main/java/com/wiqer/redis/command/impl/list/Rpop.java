package com.wiqer.redis.command.impl.list;

import com.wiqer.redis.command.CommandType;
import com.wiqer.redis.command.impl.Pop;

public class Rpop extends Pop {
    @Override
    public CommandType type() {
        return CommandType.rpop;
    }
}

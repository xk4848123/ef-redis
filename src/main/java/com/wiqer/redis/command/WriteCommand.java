package com.wiqer.redis.command;

import com.wiqer.redis.RedisCore;

public interface WriteCommand extends Command {

    void handle(RedisCore redisCore);

}

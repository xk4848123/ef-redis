package com.wiqer.redis.datatype;

/**
 * @author lilan
 */
public class RedisString implements RedisData
{
    private volatile long timeout;

    private BytesWrapper value;

    public BytesWrapper getValue()
    {
        return value;
    }

    public void setValue(BytesWrapper value)
    {
        this.value = value;
    }

    @Override
    public long timeout()
    {
        return timeout;
    }

    @Override
    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }
}

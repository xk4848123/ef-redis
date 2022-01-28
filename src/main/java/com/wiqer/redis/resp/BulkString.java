package com.wiqer.redis.resp;

import com.wiqer.redis.datatype.BytesWrapper;

public class BulkString implements Resp {

    public static final BulkString NullBulkString = new BulkString(null);

    BytesWrapper content;

    public BulkString(BytesWrapper content) {
        this.content = content;
    }

    public BytesWrapper getContent() {
        return content;
    }
}

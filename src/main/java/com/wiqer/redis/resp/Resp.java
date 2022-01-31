package com.wiqer.redis.resp;

import com.wiqer.redis.datatype.BytesWrapper;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public interface Resp {

    static void write(Resp resp, ByteBuf buffer) {
        if (resp instanceof SimpleString) {
            buffer.writeByte(RespType.STATUS.getCode());
            String content = ((SimpleString) resp).getContent();
            buffer.writeBytes(content.getBytes(StandardCharsets.UTF_8));
            buffer.writeByte(RespType.R.getCode());
            buffer.writeByte(RespType.N.getCode());
        } else if (resp instanceof Errors) {
            buffer.writeByte(RespType.ERROR.getCode());
            String content = ((Errors) resp).getContent();
            buffer.writeBytes(content.getBytes(StandardCharsets.UTF_8));
            buffer.writeByte(RespType.R.getCode());
            buffer.writeByte(RespType.N.getCode());
        } else if (resp instanceof RespInt) {
            buffer.writeByte(RespType.INTEGER.getCode());
            String content = String.valueOf(((RespInt) resp).getValue());
            buffer.writeBytes(content.getBytes(StandardCharsets.UTF_8));
            buffer.writeByte(RespType.R.getCode());
            buffer.writeByte(RespType.N.getCode());
        } else if (resp instanceof BulkString) {
            BytesWrapper content = ((BulkString) resp).getContent();
            if (content == null || content.getByteArray().length == 0) {
                buffer.writeByte(RespType.BULK.getCode());
                buffer.writeByte(RespType.ERROR.getCode());
                buffer.writeByte(RespType.ONE.getCode());
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
            } else {
                buffer.writeByte(RespType.BULK.getCode());
                String length = String.valueOf(content.getByteArray().length);
                buffer.writeBytes(length.getBytes(StandardCharsets.UTF_8));
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
                buffer.writeBytes(content.getByteArray());
                buffer.writeByte(RespType.R.getCode());
                buffer.writeByte(RespType.N.getCode());
            }
        } else if (resp instanceof RespArray) {
            buffer.writeByte(RespType.MULTYBULK.getCode());
            Resp[] array = ((RespArray) resp).getArray();
            String length = String.valueOf(array.length);
            buffer.writeBytes(length.getBytes(StandardCharsets.UTF_8));
            buffer.writeByte(RespType.R.getCode());
            buffer.writeByte(RespType.N.getCode());
            for (Resp each : array) {
                write(each, buffer);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    static Resp decode(ByteBuf buffer) {
        if (buffer.readableBytes() <= 0) {
            new IllegalStateException("没有读取到完整的命令");
        }
        char c = (char) buffer.readByte();
        if (c == RespType.STATUS.getCode()) {
            return new SimpleString(getString(buffer));
        } else if (c == RespType.ERROR.getCode()) {
            return new Errors(getString(buffer));
        } else if (c == RespType.INTEGER.getCode()) {
            int value = getNumber(buffer);
            return new RespInt(value);
        } else if (c == RespType.BULK.getCode()) {
            int length = getNumber(buffer);
            if (buffer.readableBytes() < length + 2) {
                throw new IllegalStateException("没有读取到完整的命令");
            }
            byte[] content;
            if (length == -1) {
                content = null;
            } else {
                content = new byte[length];
                buffer.readBytes(content);
            }
            if (buffer.readByte() != RespType.R.getCode() || buffer.readByte() != RespType.N.getCode()) {
                throw new IllegalStateException("没有读取到完整的命令");
            }
            return new BulkString(new BytesWrapper(content));
        } else if (c == RespType.MULTYBULK.getCode()) {
            int numOfElement = getNumber(buffer);
            Resp[] array = new Resp[numOfElement];
            for (int i = 0; i < numOfElement; i++) {
                array[i] = decode(buffer);
            }
            return new RespArray(array);
        } else if (c == '\0') {
            return null;
        } else {
            if (c > 64 && c < 91) {
                return new SimpleString(c + getString(buffer));
            } else {
                return decode(buffer);
            }
        }
    }

    static int getNumber(ByteBuf buffer) {
        char t;
        t = (char) buffer.readByte();
        boolean positive = true;
        int value = 0;
        // 错误（Errors）： 响应的首字节是 "-"
        if (t == RespType.ERROR.getCode()) {
            positive = false;
        } else {
            value = t - RespType.ZERO.getCode();
        }
        while (buffer.readableBytes() > 0 && (t = (char) buffer.readByte()) != RespType.R.getCode()) {
            value = value * 10 + (t - RespType.ZERO.getCode());
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != RespType.N.getCode()) {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        if (!positive) {
            value = -value;
        }
        return value;
    }

    static String getString(ByteBuf buffer) {
        char c;
        StringBuilder builder = new StringBuilder();
        while (buffer.readableBytes() > 0 && (c = (char) buffer.readByte()) != RespType.R.getCode()) {
            builder.append(c);
        }
        if (buffer.readableBytes() == 0 || buffer.readByte() != RespType.N.getCode()) {
            throw new IllegalStateException("没有读取到完整的命令");
        }
        return builder.toString();
    }
}

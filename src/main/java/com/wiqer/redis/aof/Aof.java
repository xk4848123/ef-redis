package com.wiqer.redis.aof;


import com.wiqer.redis.RedisCore;
import com.wiqer.redis.command.Command;
import com.wiqer.redis.command.CommandFactory;
import com.wiqer.redis.command.WriteCommand;
import com.wiqer.redis.resp.Resp;
import com.wiqer.redis.resp.RespArray;
import com.wiqer.redis.util.Format;
import com.wiqer.redis.util.PropertiesUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.log4j.Logger;
import sun.misc.Cleaner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Aof {

    private static final Logger LOGGER = Logger.getLogger(Aof.class);

    private static final String suffix = ".aof";

    public static final int shiftBit = 20;

    private Long aofPutIndex = 0L;

    private final String dir = PropertiesUtil.getAofPath();

    private final BlockingQueue<Resp> runtimeRespQueue = new LinkedBlockingQueue<>();

    private final ScheduledThreadPoolExecutor persistenceExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Aof_Single_Thread"));

    private final RedisCore redisCore;

    final ReadWriteLock reentrantLock = new ReentrantReadWriteLock();


    public Aof(RedisCore redisCore) {
        this.redisCore = redisCore;
        createAofFileDir();
        start();
    }

    private void createAofFileDir() {
        File file = new File(this.dir + suffix);
        if (!file.isDirectory()) {
            File parentFile = file.getParentFile();
            if (null != parentFile && !parentFile.exists()) {
                boolean ok = parentFile.mkdirs();
                if (ok) {
                    LOGGER.info("create aof file dir : " + dir);
                }
            }
        }
    }

    public void put(Resp resp) {
        runtimeRespQueue.offer(resp);
    }

    public void start() {
        persistenceExecutor.execute(this::pickupDiskDataAllSegment);
        persistenceExecutor.scheduleAtFixedRate(this::downDiskAllSegment, 10, 1, TimeUnit.SECONDS);
    }

    public void close() {
        try {
            persistenceExecutor.shutdown();
        } catch (Throwable t) {
            LOGGER.error("error: ", t);
        }
    }

    public void downDiskAllSegment() {
        if (reentrantLock.writeLock().tryLock()) {
            try {
                long segmentId = -1;

                Segment:
                while (segmentId != (aofPutIndex >> shiftBit)) {
                    segmentId = (aofPutIndex >> shiftBit);
                    ByteBuf bufferPolled = PooledByteBufAllocator.DEFAULT.buffer(1024);
                    RandomAccessFile randomAccessFile = new RandomAccessFile(dir + "_" + segmentId + suffix, "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    long len = channel.size();
                    int putIndex = Format.uintNBit(aofPutIndex, shiftBit);
                    long baseOffset = aofPutIndex - putIndex;

                    if (len == 0) {
                        len = 1L << shiftBit;
                    }
                    MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, len);
                    do {
                        Resp resp = runtimeRespQueue.peek();
                        if (resp == null) {
                            bufferPolled.release();
                            clean(mappedByteBuffer);
                            randomAccessFile.close();
                            break Segment;
                        }
                        Resp.write(resp, bufferPolled);
                        int respLen = bufferPolled.readableBytes();
                        int capacity = mappedByteBuffer.capacity();
                        if ((respLen + putIndex >= capacity)) {
                            bufferPolled.release();
                            clean(mappedByteBuffer);
                            randomAccessFile.close();
                            aofPutIndex = baseOffset + (1 << shiftBit);
                            break;
                        }

                        while (respLen > 0) {
                            respLen--;
                            mappedByteBuffer.put(putIndex++, bufferPolled.readByte());
                        }
                        aofPutIndex = baseOffset + putIndex;
//                        runtimeRespQueue.poll();

                        bufferPolled.clear();
                    } while (true);

                }

            } catch (IOException e) {
                System.err.println(e.getMessage());
                LOGGER.error("aof IOException ", e);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                LOGGER.error("aof Exception", e);
            } finally {
                reentrantLock.writeLock().unlock();
            }

        }
    }

    public void pickupDiskDataAllSegment() {
        reentrantLock.writeLock().lock();
        try {
            long segmentId = -1;

            Segment:
            while (segmentId != (aofPutIndex >> shiftBit)) {
                segmentId = (aofPutIndex >> shiftBit);
                RandomAccessFile randomAccessFile = new RandomAccessFile(dir + "_" + segmentId + suffix, "r");
                FileChannel channel = randomAccessFile.getChannel();
                long len = channel.size();
                int putIndex = Format.uintNBit(aofPutIndex, shiftBit);
                long baseOffset = aofPutIndex - putIndex;

                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, len);
                ByteBuf bufferPolled = PooledByteBufAllocator.DEFAULT.buffer((int) len);
                bufferPolled.writeBytes(mappedByteBuffer);

                do {
                    Resp resp;
                    try {
                        resp = Resp.decode(bufferPolled);
                        if (resp == null){
                            bufferPolled.release();
                            clean(mappedByteBuffer);
                            aofPutIndex = baseOffset + (1 << shiftBit);
                            randomAccessFile.close();
                            break;
                        }
                    } catch (Throwable t) {
                        clean(mappedByteBuffer);
                        randomAccessFile.close();
                        bufferPolled.release();
                        break Segment;
                    }
                    assert resp instanceof RespArray;
                    Command command = CommandFactory.from((RespArray) resp);
                    WriteCommand writeCommand = (WriteCommand) command;
                    assert writeCommand != null;
                    writeCommand.handle(this.redisCore);
                    putIndex = bufferPolled.readerIndex();
                    System.out.println(putIndex);
                    aofPutIndex = putIndex + baseOffset;
                    if (putIndex >= (1 << shiftBit)) {
                        bufferPolled.release();
                        clean(mappedByteBuffer);
                        aofPutIndex = baseOffset + (1 << shiftBit);
                        randomAccessFile.close();
                        break;
                    }
                } while (true);

            }
            LOGGER.info("read aof end");
        } catch (Throwable t) {
            if (t instanceof FileNotFoundException) {
                LOGGER.info("read aof end");
            } else {
                LOGGER.error("read aof error: ", t);
            }
        } finally {
            reentrantLock.writeLock().unlock();
        }

    }


    /*
     * 其实讲到这里该问题的解决办法已然清晰明了了——就是在删除索引文件的同时还取消对应的内存映射，删除mapped对象。
     * 不过令人遗憾的是，Java并没有特别好的解决方案——令人有些惊讶的是，Java没有为MappedByteBuffer提供unmap的方法，
     * 该方法甚至要等到Java 10才会被引入 ,DirectByteBufferR类是不是一个公有类
     * class DirectByteBufferR extends DirectByteBuffer implements DirectBuffer 使用默认访问修饰符
     * 不过Java倒是提供了内部的“临时”解决方案——DirectByteBufferR.cleaner().clean() 切记这只是临时方法，
     * 毕竟该类在Java9中就正式被隐藏了，而且也不是所有JVM厂商都有这个类。
     * 还有一个解决办法就是显式调用System.gc()，让gc赶在cache失效前就进行回收。
     * 不过坦率地说，这个方法弊端更多：首先显式调用GC是强烈不被推荐使用的，
     * 其次很多生产环境甚至禁用了显式GC调用，所以这个办法最终没有被当做这个bug的解决方案。
     */
    public static void clean(final MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        buffer.force();
        AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
                cleaner.clean();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }
}

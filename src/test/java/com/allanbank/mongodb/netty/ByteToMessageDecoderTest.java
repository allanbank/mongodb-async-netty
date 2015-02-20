/*
 * #%L
 * ByteToMessageDecoderTest.java - mongodb-async-netty - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2015 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.netty;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.SlicedByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

import java.util.Collections;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;

/**
 * ByteToMessageDecoderTest provides tests for the {@link ByteToMessageDecoder}
 * class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class ByteToMessageDecoderTest {

    /** The test's allocator of {@link ByteBuf} instances. */
    private static final ByteBufAllocator ourAllocator = UnpooledByteBufAllocator.DEFAULT;

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeBadOpCode() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);
        final int msgId = rand.nextInt() & 0xFFFFFF;

        final ByteBuf buffer = ourAllocator.buffer();

        final ByteBufOutputStream out = new ByteBufOutputStream(buffer);
        final BsonOutputStream bout = new BsonOutputStream(out);

        msg.write(msgId, bout);

        // OpCode is bytes 12-16.
        buffer.setByte(12, (byte) 0xAA);
        buffer.setByte(13, (byte) 0xBB);
        buffer.setByte(14, (byte) 0xCC);
        buffer.setByte(15, (byte) 0xDD);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);

        replay(mockContext);

        final ByteToMessageDecoder decoder = new ByteToMessageDecoder(
                new StringDecoderCache());

        try {
            decoder.decode(mockContext, out.buffer());
            fail("Should have thrown a MongoDBException.");
        }
        catch (final MongoDbException good) {
            assertThat(good.getMessage(), is("Unexpected operation read '"
                    + 0xDDCCBBAA + "'."));
        }

        verify(mockContext);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeDelete() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Delete("db", "c", Find.ALL, rand.nextBoolean());

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeGetMore() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new GetMore("db", "c", rand.nextLong(),
                rand.nextInt(), ReadPreference.PRIMARY);

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeInCompleteFrame() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);
        final int msgId = rand.nextInt() & 0xFFFFFF;

        final ByteBuf buffer = ourAllocator.buffer();

        final ByteBufOutputStream out = new ByteBufOutputStream(buffer);
        final BsonOutputStream bout = new BsonOutputStream(out);

        msg.write(msgId, bout);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);

        replay(mockContext);

        final ByteToMessageDecoder decoder = new ByteToMessageDecoder(
                new StringDecoderCache());

        final Object result = decoder.decode(mockContext,
                buffer.slice(0, buffer.writerIndex() - 1));
        assertThat(result, nullValue());

        verify(mockContext);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeInsert() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Insert("db", "c",
                Collections.singletonList(Find.ALL), rand.nextBoolean());

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeKillCursors() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeQuery() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Query("db", "c", Find.ALL, null, 0, 0,
                rand.nextInt() & 0xFFFFFF, rand.nextBoolean(),
                ReadPreference.PRIMARY, rand.nextBoolean(), rand.nextBoolean(),
                rand.nextBoolean(), rand.nextBoolean());

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeReply() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Reply reply = new Reply(123, 456, 789,
                Collections.singletonList(BuilderFactory.d().asDocument()),
                true, false, false, false);

        runDecode(reply, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#decode(ChannelHandlerContext, ByteBuf)}.
     *
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testDecodeUpdate() throws Exception {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new Update("db", "collection", Find.ALL, Find.ALL,
                true, false);

        runDecode(msg, rand);
    }

    /**
     * Test method for
     * {@link ByteToMessageDecoder#extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
     * .
     */
    @Test
    public void testExtractFrameChannelHandlerContextByteBufIntInt() {
        final ByteBuf buffer = ourAllocator.buffer();

        buffer.writeBytes(new byte[1000]);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);

        replay(mockContext);

        final ByteToMessageDecoder decoder = new ByteToMessageDecoder(
                new StringDecoderCache());

        final ByteBuf result = decoder.extractFrame(mockContext, buffer, 100,
                200);
        assertThat(result, instanceOf(SlicedByteBuf.class));

        assertThat(buffer.getByte(100), is((byte) 0));
        result.setByte(0, 1);
        assertThat(buffer.getByte(100), is((byte) 1));

        verify(mockContext);
    }

    /**
     * Performs the basic receive test with the message.
     *
     * @param message
     *            The message to send.
     * @param random
     *            The source of random for the test.
     * @throws Exception
     *             On a test failure.
     */
    private void runDecode(final Message message, final Random random)
            throws Exception {
        final ByteBuf buffer = ourAllocator.buffer();

        final ByteBufOutputStream out = new ByteBufOutputStream(buffer);
        final BsonOutputStream bout = new BsonOutputStream(out);

        final int msgId = random.nextInt() & 0xFFFFFF;
        message.write(msgId, bout);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);

        replay(mockContext);

        final ByteToMessageDecoder decoder = new ByteToMessageDecoder(
                new StringDecoderCache());

        final Object result = decoder.decode(mockContext, out.buffer());
        assertThat(result, is((Object) message));

        verify(mockContext);
    }
}

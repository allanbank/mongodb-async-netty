/*
 * #%L
 * NettyOutputBufferTest.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.KillCursors;

/**
 * NettyOutputBufferTest provides tests for the {@link NettyOutputBuffer} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class NettyOutputBufferTest {

    /** The test's allocator of {@link ByteBuf} instances. */
    private static final ByteBufAllocator ourAllocator = UnpooledByteBufAllocator.DEFAULT;

    /**
     * Test method for {@link NettyOutputBuffer#close()}.
     */
    @Test
    public void testClose() {
        final ByteBuf mockBuffer = createMock(ByteBuf.class);

        expect(mockBuffer.writerIndex()).andReturn(0);
        expect(mockBuffer.release()).andReturn(true);

        replay(mockBuffer);

        final NettyOutputBuffer outBuffer = new NettyOutputBuffer(mockBuffer,
                new StringEncoderCache());

        assertThat(outBuffer.getBuffer(), sameInstance(mockBuffer));

        outBuffer.close();

        verify(mockBuffer);
    }

    /**
     * Test method for {@link NettyOutputBuffer#write}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testWrite() throws IOException {

        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);
        final int msgId = rand.nextInt() & 0xFFFFFF;

        final ByteBuf expected = ourAllocator.buffer();
        final ByteBufOutputStream out = new ByteBufOutputStream(expected);
        final BsonOutputStream bout = new BsonOutputStream(out);
        msg.write(msgId, bout);

        final ByteBuf buffer = ourAllocator.buffer();
        final NettyOutputBuffer outBuffer = new NettyOutputBuffer(buffer,
                new StringEncoderCache());

        outBuffer.write(msgId, msg, null);

        assertThat(buffer, is(expected));

        outBuffer.close();
    }

}

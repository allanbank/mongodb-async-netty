/*
 * #%L
 * NettyOutputBuffer.java - mongodb-async-netty - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2014 - 2015 Allanbank Consulting, Inc.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;

import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;

/**
 * NettyOutputBuffer provides the buffer for serializing messages to send via
 * the Netty pipeline.
 *
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class NettyOutputBuffer implements TransportOutputBuffer {

    /** The ByteBuf baking this buffer. */
    private final ByteBuf myBackingBuffer;

    /** The cache for encoding strings. */
    private final StringEncoderCache myCache;

    /** The output stream wrapping the ByteBuf. */
    private final ByteBufOutputStream myOutstream;

    /**
     * Creates a new NettyOutputBuffer.
     *
     * @param buffer
     *            The backing {@link ByteBuf}.
     * @param cache
     *            The cache for encoding strings.
     */
    public NettyOutputBuffer(final ByteBuf buffer,
            final StringEncoderCache cache) {
        myBackingBuffer = buffer;
        myCache = cache;

        myOutstream = new ByteBufOutputStream(myBackingBuffer);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to release the backing buffer.
     * </p>
     */
    @Override
    public void close() {
        myBackingBuffer.release();
    }

    /**
     * Returns the backing {@link ByteBuf}.
     *
     * @return The backing {@link ByteBuf}.
     */
    public ByteBuf getBuffer() {
        return myBackingBuffer;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the message to the backing {@link ByteBuf}.
     * </p>
     */
    @Override
    public void write(final int messageId, final Message message,
            final ReplyCallback callback) throws IOException {

        final BsonOutputStream bout = new BsonOutputStream(myOutstream, myCache);

        message.write(messageId, bout);
    }
}

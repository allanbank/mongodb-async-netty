/*
 * #%L
 * ByteToMessageDecoder.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.nio.ByteOrder;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.Header;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.error.ConnectionLostException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * MessagesToByteEncoder provides a handler to turn an array of pending messages
 * into the buffer to be sent.
 *
 * @copyright 2014-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ByteToMessageDecoder extends LengthFieldBasedFrameDecoder {

    /**
     * Flag to cause the {@link LengthFieldBasedFrameDecoder} to fail on an
     * error.
     */
    private static final boolean FAIL_FAST = true;

    /** The number of initial bytes to strip from messages: {@value} . */
    private static final int INITIAL_BYTES_TO_STRIP = 0;

    /** The amount to adjust the length parsed from the messages: {@value} . */
    private static final int LENGTH_ADJUSTMENT = -4;

    /** The number of bytes for the length: {@value} . */
    private static final int LENGTH_FIELD_LENGTH = 4;

    /** The offset of the length field: {@value} . */
    private static final int LENGTH_FIELD_OFFSET = 0;

    /** The maximum length for a frame: {@value} . */
    private static final int MAX_FRAME_LENGTH = Integer.MAX_VALUE;

    /** The cache for decoding strings. */
    private final StringDecoderCache myDecoderCache;

    /**
     * Creates a new ByteToMessageDecoder.
     *
     * @param decoderCache
     *            The cache used to decode Strings.
     */
    public ByteToMessageDecoder(final StringDecoderCache decoderCache) {
        super(ByteOrder.LITTLE_ENDIAN, MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET,
                LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP,
                FAIL_FAST);

        myDecoderCache = decoderCache;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to read the messages from the {@link ByteBuf}.
     * </p>
     */
    @Override
    protected Object decode(final ChannelHandlerContext ctx, final ByteBuf in)
            throws Exception {

        final ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        final ByteBufInputStream instream = new ByteBufInputStream(frame);
        final BsonInputStream bin = new BsonInputStream(instream,
                myDecoderCache);

        try {
            final int length = bin.readInt();

            final int requestId = bin.readInt();
            final int responseId = bin.readInt();
            final int opCode = bin.readInt();

            final Operation op = Operation.fromCode(opCode);
            if (op == null) {
                // Huh? Dazed and confused
                throw new MongoDbException("Unexpected operation read '"
                        + opCode + "'.");
            }

            final Header header = new Header(length, requestId, responseId, op);
            Message message = null;
            switch (op) {
            case REPLY:
                message = new Reply(header, bin);
                break;
            case QUERY:
                message = new Query(header, bin);
                break;
            case UPDATE:
                message = new Update(bin);
                break;
            case INSERT:
                message = new Insert(header, bin);
                break;
            case GET_MORE:
                message = new GetMore(bin);
                break;
            case DELETE:
                message = new Delete(bin);
                break;
            case KILL_CURSORS:
                message = new KillCursors(bin);
                break;
            }

            return message;
        }
        catch (final IOException ioe) {
            final MongoDbException error = new ConnectionLostException(ioe);

            // Not good. Close the connection.
            ctx.close();

            throw error;
        }
        finally {
            IOUtils.close(bin);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a slice of the buffer since it will not escape the
     * current stack.
     * </p>
     */
    @Override
    protected ByteBuf extractFrame(final ChannelHandlerContext ctx,
            final ByteBuf buffer, final int index, final int length) {
        return buffer.slice(index, length);
    }
}

/*
 * #%L
 * MessagesToByteEncoder.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * MessagesToByteEncoder provides a handler to turn an array of pending messages
 * into the buffer to be sent.
 * 
 * @copyright 2014-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MessagesToByteEncoder extends
        MessageToByteEncoder<PendingMessage[]> {

    /** The logger for the encoder. */
    private static final Log LOG = LogFactory
            .getLog(MessagesToByteEncoder.class);

    /**
     * Creates a new MessagesToByteEncoder.
     */
    public MessagesToByteEncoder() {
        super(PendingMessage[].class);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to send the {@link PendingMessage PendingMessages}.
     * </p>
     */
    @Override
    protected void encode(final ChannelHandlerContext ctx,
            final PendingMessage[] msgs, final ByteBuf out) throws Exception {
        final ByteBufOutputStream outstream = new ByteBufOutputStream(out);
        final BsonOutputStream bout = new BsonOutputStream(outstream);

        for (final PendingMessage message : msgs) {
            LOG.debug("{} send {} - {}", message.getMessageId(), message
                    .getMessage().getClass().getSimpleName(),
                    (message.getReplyCallback() != null));
            message.getMessage().write(message.getMessageId(), bout);
        }
    }
}

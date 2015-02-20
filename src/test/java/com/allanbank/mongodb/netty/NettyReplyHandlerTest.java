/*
 * #%L
 * NettyReplyHandlerTest.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import io.netty.channel.ChannelHandlerContext;

import java.util.Random;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.transport.TransportInputBuffer;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.MessageInputBuffer;

/**
 * NettyReplyHandlerTest provides tests for the {@link NettyReplyHandler} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class NettyReplyHandlerTest {

    /**
     * Test method for
     * {@link NettyReplyHandler#channelRead(ChannelHandlerContext, Object)}.
     */
    @Test
    public void testChannelRead() {
        final Random rand = new Random(System.currentTimeMillis());
        final Message msg = new KillCursors(new long[] { rand.nextLong() },
                ReadPreference.PRIMARY);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);
        final Capture<TransportInputBuffer> capture = new Capture<TransportInputBuffer>();

        mockListener.response(capture(capture));

        replay(mockContext, mockListener);

        final NettyReplyHandler handler = new NettyReplyHandler(mockListener);

        handler.channelRead(mockContext, msg);

        verify(mockContext, mockListener);

        assertThat(capture.getValue(), instanceOf(MessageInputBuffer.class));
        assertThat(((MessageInputBuffer) capture.getValue()).read(), is(msg));
    }

    /**
     * Test method for
     * {@link NettyReplyHandler#channelRead(ChannelHandlerContext, Object)}.
     */
    @Test
    public void testChannelReadNonMessage() {
        final Object msg = Integer.valueOf(1);

        final ChannelHandlerContext mockContext = createMock(ChannelHandlerContext.class);
        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);

        expect(mockContext.close()).andReturn(null);

        replay(mockContext, mockListener);

        final NettyReplyHandler handler = new NettyReplyHandler(mockListener);

        handler.channelRead(mockContext, msg);

        verify(mockContext, mockListener);
    }
}

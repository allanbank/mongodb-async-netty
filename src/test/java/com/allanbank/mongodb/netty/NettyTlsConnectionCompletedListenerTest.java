/*
 * #%L
 * NettyTlsConnectionCompletedListenerTest.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketException;

import javax.net.ssl.SSLEngine;

import org.junit.Test;

import com.allanbank.mongodb.client.connection.SocketConnectionListener;

/**
 * NettyTlsConnectionCompletedListenerTest provides tests for the
 * {@link NettyTlsConnectionCompletedListener} class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class NettyTlsConnectionCompletedListenerTest {

    /**
     * Test for the
     * {@link NettyTlsConnectionCompletedListener#operationComplete} method.
     *
     * @throws SocketException
     *             On a test failure.
     */
    @Test
    public void testOperationComplete() throws SocketException {
        final InetSocketAddress addr = new InetSocketAddress(12346);

        final SocketChannel mockChannel = createMock(SocketChannel.class);
        final SocketConnectionListener mockConnListener = createMock(SocketConnectionListener.class);
        final SSLEngine mockEngine = createMock(SSLEngine.class);

        expect(mockChannel.remoteAddress()).andReturn(addr);
        mockConnListener.connected(addr, mockEngine);
        expectLastCall();

        replay(mockChannel, mockConnListener, mockEngine);

        final NettyTlsConnectionCompletedListener listener = new NettyTlsConnectionCompletedListener(
                mockConnListener, mockEngine, mockChannel);

        listener.operationComplete(null);

        verify(mockChannel, mockConnListener, mockEngine);
    }

    /**
     * Test for the
     * {@link NettyTlsConnectionCompletedListener#operationComplete} method.
     *
     * @throws SocketException
     *             On a test failure.
     */
    @Test
    public void testOperationCompleteOnError() throws SocketException {
        final InetSocketAddress addr = new InetSocketAddress(12346);

        final SocketChannel mockChannel = createMock(SocketChannel.class);
        final SocketConnectionListener mockConnListener = createMock(SocketConnectionListener.class);
        final SSLEngine mockEngine = createMock(SSLEngine.class);

        expect(mockChannel.remoteAddress()).andReturn(addr);
        mockConnListener.connected(addr, mockEngine);
        expectLastCall().andThrow(new SocketException("Injected."));
        expect(mockChannel.close()).andReturn(null);

        replay(mockChannel, mockConnListener, mockEngine);

        final NettyTlsConnectionCompletedListener listener = new NettyTlsConnectionCompletedListener(
                mockConnListener, mockEngine, mockChannel);

        listener.operationComplete(null);

        verify(mockChannel, mockConnListener, mockEngine);
    }
}

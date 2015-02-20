/*
 * #%L
 * NettyTlsConnectionCompletedListener.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.SocketException;

import javax.net.ssl.SSLEngine;

import com.allanbank.mongodb.client.connection.SocketConnectionListener;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * NettyTlsConnectionCompletedListener provides a listener to verify the
 * connected to host when the TLS handshake completes.
 *
 * @copyright 2014-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class NettyTlsConnectionCompletedListener implements
GenericFutureListener<Future<Channel>> {
    /** The logger for the listener. */
    private static final Log LOG = LogFactory
            .getLog(NettyTlsConnectionCompletedListener.class);

    /** The underlying Netty socket channel. */
    private final SocketChannel myChannel;

    /** The SocketConnectionListener to notify. */
    private final SocketConnectionListener myConnListener;

    /** The TLS Engine being used. */
    private final SSLEngine myTlsEngine;

    /**
     * Creates a new {@link NettyTlsConnectionCompletedListener}.
     *
     * @param connListener
     *            The SocketConnectionListener to notify.
     * @param tlsEngine
     *            The TLS Engine being used.
     * @param channel
     *            The underlying Netty socket channel.
     */
    public NettyTlsConnectionCompletedListener(
            final SocketConnectionListener connListener,
            final SSLEngine tlsEngine, final SocketChannel channel) {
        myConnListener = connListener;
        myTlsEngine = tlsEngine;
        myChannel = channel;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to notify the connection listener that the TLS handshake is
     * complete.
     * </p>
     */
    @Override
    public void operationComplete(final Future<Channel> future) {
        try {
            myConnListener.connected(myChannel.remoteAddress(), myTlsEngine);
        }
        catch (final SocketException se) {
            // TLS Hostname failure.
            LOG.warn(se, "Hostname verification failure: {}", se.getMessage());
            myChannel.close();
        }
    }
}
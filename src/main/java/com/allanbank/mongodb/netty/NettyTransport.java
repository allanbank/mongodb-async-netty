/*
 * #%L
 * NettyTransport.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.callback.Receiver;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * NettySocketConnection provides a channel representing the connection to the
 * server.
 *
 * @copyright 2014-2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NettyTransport implements Transport<NettyOutputBuffer>, Receiver {

    /** The channel for the Connection to the server. */
    private final Channel myChannel;

    /** The cache for encoding strings. */
    private final StringEncoderCache myEncoderCache;

    /**
     * Creates a new NettySocketConnection.
     *
     * @param server
     *            The MongoDB server to connect to.
     * @param config
     *            The configuration for the Connection to the MongoDB server.
     * @param encoderCache
     *            Cache used for encoding strings.
     * @param decoderCache
     *            Cache used for decoding strings.
     * @param responseListener
     *            The listener for the status of the transport/connection.
     * @param bootstrap
     *            The seeded bootstrap for creating the {@link Channel}.
     *
     * @throws IOException
     *             On a failure connecting to the MongoDB server.
     */
    public NettyTransport(final Server server,
            final MongoClientConfiguration config,
            final StringEncoderCache encoderCache,
            final StringDecoderCache decoderCache,
            final TransportResponseListener responseListener,
            final Bootstrap bootstrap) throws IOException {

        myEncoderCache = encoderCache;

        // These are critical settings for performance/function.
        bootstrap.option(ChannelOption.TCP_NODELAY, Boolean.TRUE);
        bootstrap.option(ChannelOption.AUTO_READ, Boolean.TRUE);

        bootstrap.handler(new NettyChannelInit(config, decoderCache,
                responseListener));

        Channel channel = null;
        final Iterator<InetSocketAddress> addrIter = server.getAddresses()
                .iterator();
        IOException error = null;
        while (addrIter.hasNext() && (channel == null)) {
            final InetSocketAddress address = addrIter.next();
            try {
                channel = bootstrap.connect(address).await().channel();
            }
            catch (final InterruptedException e) {
                error = new IOException(
                        "Failed to wait for the connection to complete.", e);
            }
            catch (final RuntimeException e) {
                error = new IOException("Failed to create a connection to '"
                        + address + "'.", e);
            }
        }

        if (channel != null) {
            myChannel = channel;
        }
        else if (error != null) {
            throw error;
        }
        else {
            throw new IOException(
                    "Failed to create a connection to the server: "
                            + server.getAddresses());
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the Netty channel backing this connection.
     * </p>
     */
    @Override
    public void close() throws IOException {
        myChannel.close();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to use the channel's {@link ByteBufAllocator} to allocate a
     * buffer of the specified size.
     * </p>
     */
    @Override
    public NettyOutputBuffer createSendBuffer(final int size) {
        return new NettyOutputBuffer(myChannel.alloc().buffer(size),
                myEncoderCache);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to flush the Netty channel backing this connection.
     * </p>
     */
    @Override
    public void flush() throws IOException {
        myChannel.flush();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the buffer to the channel.
     * </p>
     */
    @Override
    public void send(final NettyOutputBuffer buffer) throws IOException,
            InterruptedIOException {
        myChannel.write(buffer.getBuffer());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void start() {
        // Nothing to do.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        final SocketAddress local = myChannel.localAddress();
        if (local instanceof InetSocketAddress) {
            return "MongoDB(" + ((InetSocketAddress) local).getPort() + "-->"
                    + myChannel.remoteAddress() + ")";

        }
        return "MongoDB(" + local + "-->" + myChannel.remoteAddress() + ")";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to try and perform a read on the channel.
     * </p>
     */
    @Override
    public void tryReceive() {
        if (myChannel.eventLoop().inEventLoop()) {
            myChannel.read();
        }
    }
}

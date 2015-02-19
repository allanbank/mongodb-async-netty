/*
 * #%L
 * NettyChannelInit.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.IOException;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.client.connection.SocketConnectionListener;
import com.allanbank.mongodb.client.transport.SslEngineFactory;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.util.IOUtils;

/**
 * NettyChannelInit provides a callback to initialize the socket channels.
 * 
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
final class NettyChannelInit extends ChannelInitializer<SocketChannel> {

    /** The configuration for initializing the channel. */
    private final MongoClientConfiguration myClientConfig;

    /** The listener for responses from the server. */
    private final TransportResponseListener myResponseListener;

    /** The cache for decoding strings. */
    private final StringDecoderCache myDecoderCache;

    /**
     * Creates a new ChannelInit.
     * 
     * @param config
     *            The {@link MongoClientConfiguration} to configure the client's
     *            socket.
     * @param decoderCache
     *            The cache for decoding strings.
     * @param listener
     *            The listener for recieved messages and socket events.
     */
    protected NettyChannelInit(final MongoClientConfiguration config,
            StringDecoderCache decoderCache, TransportResponseListener listener) {
        myClientConfig = config;
        myDecoderCache = decoderCache;
        myResponseListener = listener;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to initialize the channel's processing pipeline.
     * </p>
     */
    @Override
    public void initChannel(final SocketChannel ch) throws Exception {

        final ChannelPipeline pipeline = ch.pipeline();

        // Make sure we know when the connection gets closed.
        ch.closeFuture()
                .addListener(new NettyCloseListener(myResponseListener));

        SSLEngine engine = null;
        final SocketFactory socketFactory = myClientConfig.getSocketFactory();
        if (socketFactory instanceof SslEngineFactory) {

            final SslEngineFactory factory = (SslEngineFactory) socketFactory;
            engine = factory.createSSLEngine();

        }
        else if (socketFactory instanceof SSLSocketFactory) {
            engine = createVanillaEngine((SSLSocketFactory) socketFactory);
        }

        if (engine != null) {
            engine.setUseClientMode(true);

            SslHandler handler = new SslHandler(engine, false /* startTLS */);
            pipeline.addLast("ssl", handler);

            if (socketFactory instanceof SocketConnectionListener) {
                handler.handshakeFuture().addListener(
                        new NettyTlsConnectionCompletedListener(
                                (SocketConnectionListener) socketFactory,
                                engine, ch));
            }
        }

        // Write side.
        pipeline.addLast("messageToBufHandler", new MessagesToByteEncoder());

        // Read side.
        pipeline.addLast("readTimeoutHandler", new ReadTimeoutHandler(
                myClientConfig.getReadTimeout(), TimeUnit.MILLISECONDS));
        pipeline.addLast("bufToMessageHandler", new ByteToMessageDecoder(
                myDecoderCache));
        pipeline.addLast("replyHandler", new NettyReplyHandler(
                myResponseListener));
    }

    /**
     * Uses the {@link SSLSocketFactory} to create an {@link SSLSocket} that we
     * can then use the {@link SSLParameters} from to create and appropriately
     * configured {@link SSLEngine}.
     * 
     * @param sslFactory
     *            The factory to create a {@link SSLEngine} from.
     * @return The {@link SSLEngine} created with the parameters from the
     *         {@link SSLSocketFactory}.
     * @throws IOException
     *             On an I/O error.
     * @throws NoSuchAlgorithmException
     *             On a failure to create an {@link SSLEngine}.
     */
    private SSLEngine createVanillaEngine(SSLSocketFactory sslFactory)
            throws IOException, NoSuchAlgorithmException {

        // Pull the user's parameters from a socket created by the
        // factory.
        SSLEngine engine = null;
        Socket socket = null;
        try {
            socket = sslFactory.createSocket();
            if (socket instanceof SSLSocket) {
                SSLParameters params = ((SSLSocket) socket).getSSLParameters();

                for (String protocol : params.getProtocols()) {
                    SSLContext context;
                    try {
                        context = SSLContext.getInstance(protocol);
                        engine = context.createSSLEngine();
                        break;
                    }
                    catch (NoSuchAlgorithmException e) {
                        // Try the next protocol.
                    }
                }

                if (engine != null) {
                    engine.setSSLParameters(params);
                }
            }
        }
        finally {
            IOUtils.close(socket);

            if (engine == null) {
                engine = SSLContext.getDefault().createSSLEngine();
            }
        }

        return engine;
    }
}
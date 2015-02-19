/*
 * #%L
 * NettyTransportFactory.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.bson.io.StringDecoderCache;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.state.Server;
import com.allanbank.mongodb.client.transport.Transport;
import com.allanbank.mongodb.client.transport.TransportFactory;
import com.allanbank.mongodb.client.transport.TransportResponseListener;

/**
 * NettyTransportFactory provides factory to create connections to MongoDB via
 * Netty {@link Channel channels}.
 * 
 * @copyright 2014-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NettyTransportFactory implements TransportFactory {

    /** The channel for the Connection to the server. */
    private final EventLoopGroup myGroup;

    /** The {@link ByteBufAllocator} for the transport. */
    private final ByteBufAllocator myBufferAllocator;

    /**
     * Creates a new NettyTransportFactory.
     */
    public NettyTransportFactory() {
        this(new NioEventLoopGroup(), PooledByteBufAllocator.DEFAULT);
    }

    /**
     * Creates a new NettyTransportFactory.
     * 
     * @param group
     *            The client's configuration.
     * @param bufferAllocator
     *            The {@link ByteBufAllocator} to share across all connections
     *            created.
     */
    public NettyTransportFactory(EventLoopGroup group,
            ByteBufAllocator bufferAllocator) {
        myGroup = group;
        myBufferAllocator = bufferAllocator;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to create a {@link NettyTransport}.
     * </p>
     */
    @Override
    public Transport<?> createTransport(Server server,
            MongoClientConfiguration config, StringEncoderCache encoderCache,
            StringDecoderCache decoderCache,
            TransportResponseListener responseListener) throws IOException {

        return new NettyTransport(server, config, encoderCache, decoderCache,
                responseListener, createBootstrap(config));
    }

    /**
     * Creates a new {@link Bootstrap} for creating a Netty client
     * {@link Channel channels}.
     * 
     * @param config
     *            The configuration for the client.
     * @return The {@link Bootstrap} to create Netty client channels.
     */
    protected Bootstrap createBootstrap(MongoClientConfiguration config) {
        final Bootstrap bootstrap = new Bootstrap();

        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.ALLOCATOR, myBufferAllocator);
        bootstrap.group(myGroup);

        // Suggested defaults.
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        
        // Settings from the config.
        bootstrap.option(ChannelOption.SO_KEEPALIVE,
                Boolean.valueOf(config.isUsingSoKeepalive()));
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                Integer.valueOf(config.getConnectTimeout()));

        return bootstrap;
    }
}

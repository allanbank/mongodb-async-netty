/*
 * #%L
 * NettyReplyHandler.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.client.transport.bio.MessageInputBuffer;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * NettyReplyHandler provides a class to handle replies in the Netty pipeline.
 * 
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
final class NettyReplyHandler extends ChannelInboundHandlerAdapter {

	/** The logger for the transport. */
	private static final Log LOG = LogFactory.getLog(NettyReplyHandler.class);

	/** The listener for the responses. */
	private final TransportResponseListener myResponseListener;

	/**
	 * Creates a new NettyReplyHandler.
	 * 
	 * @param listener
	 *            The listener for the responses.
	 */
	public NettyReplyHandler(TransportResponseListener listener) {
		super();
		myResponseListener = listener;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * If the message is a {@link Message} then forwards the message to the
	 * {@link TransportResponseListener}.
	 * </p>
	 */
	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg)
			throws Exception {
		if (msg instanceof Message) {
			myResponseListener
					.response(new MessageInputBuffer(((Message) msg)));
		} else {
			LOG.warn("Received a non-message: {}.", msg);
			ctx.close();
		}
	}
}
/*
 * #%L
 * NettyCloseListener.java - mongodb-async-netty - Allanbank Consulting, Inc.
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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.MongoClientClosedException;

/**
 * NettyCloseListener provides the listener for notification that the Netty
 * channel has closed.
 * 
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class NettyCloseListener implements
		GenericFutureListener<Future<Void>> {

	/** The listener for the connection closing. */
	private final TransportResponseListener myResponseListener;

	/**
	 * Creates a new NettyCloseListener.
	 * 
	 * @param responseListener
	 *            The listener for the connection closing.
	 */
	public NettyCloseListener(TransportResponseListener responseListener) {
		myResponseListener = responseListener;
	}

	/**
	 * Notification that the Netty channel is closed.
	 * 
	 * @param future
	 *            The close future - ignored.
	 */
	@Override
	public void operationComplete(final Future<Void> future) throws Exception {
		myResponseListener.closed(new MongoClientClosedException(
				"Client connection has been closed."));
	}
}
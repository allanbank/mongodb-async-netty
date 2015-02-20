/*
 * #%L
 * NettyCloseListenerTest.java - mongodb-async-netty - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.transport.TransportResponseListener;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * NettyCloseListenerTest provides tests for the {@link NettyCloseListener}
 * class.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */

public class NettyCloseListenerTest {

    /**
     * Test method for {@link NettyCloseListener#operationComplete}.
     */
    @Test
    public void testOperationComplete() {

        final TransportResponseListener mockListener = createMock(TransportResponseListener.class);
        final Capture<MongoDbException> capture = new Capture<MongoDbException>();

        mockListener.closed(EasyMock.capture(capture));

        replay(mockListener);

        final NettyCloseListener listener = new NettyCloseListener(mockListener);

        listener.operationComplete(null);

        verify(mockListener);

        assertThat(capture.getValue(),
                instanceOf(ConnectionLostException.class));
    }
}

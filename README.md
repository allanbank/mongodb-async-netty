[![Build Status](https://travis-ci.org/allanbank/mongodb-async-netty.svg?branch=master)](https://travis-ci.org/allanbank/mongodb-async-netty)
MongoDB Asynchronous Java Driver Adapter for Netty
=============================================

This project contains the adapter for using Netty as the low level transport the MongoDB Asynchronous Java Driver.

For more information on the MongoDB Asynchronous Java Driver see the driver's [website](http://www.allanbank.com/mongodb-async-driver/).

# Netty Transport

To use the Netty transport with the driver simply set the Netty transport factory in the MongoConfiguration used to create the MongoClient.

    EventLoopGroup group = ...;
    ByteBufAllocator bufferAllocator = ...;
    MongoClientConfiguration mongodbConfig = new MongoClientConfiguration();
    mongodbConfig.setTransportFactory(new NettyTransportFactory(group, bufferAllocator));
    
    MongoClient mongoClient = MongoFactory.createClient(mongodbConfig);


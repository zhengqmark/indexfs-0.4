// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_IPC_RPCTYPES_H_
#define _INDEXFS_IPC_RPCTYPES_H_

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
namespace indexfs {
using boost::weak_ptr;
using boost::shared_ptr;
using boost::scoped_ptr;
}

#include <thrift/TProcessor.h>
namespace indexfs {
using apache::thrift::TProcessor;
}

#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
namespace indexfs {
using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocolFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;
}

#include <thrift/server/TServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/server/TNonblockingServer.h>
namespace indexfs {
using apache::thrift::server::TServer;
using apache::thrift::server::TThreadedServer;
using apache::thrift::server::TNonblockingServer;
}

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
namespace indexfs {
using apache::thrift::concurrency::ThreadManager;
using apache::thrift::concurrency::PosixThreadFactory;
}

#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransportUtils.h>
namespace indexfs {
using apache::thrift::transport::TTransportException;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TBufferedTransportFactory;
}

#endif /* _INDEXFS_IPC_RPCTYPES_H_ */

/****************************************************************************
** Copyright (c) 2001-2014
**
** This file is part of the QuickFIX FIX Engine
**
** This file may be distributed under the terms of the quickfixengine.org
** license as defined by quickfixengine.org and appearing in the file
** LICENSE included in the packaging of this file.
**
** This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING THE
** WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
**
** See http://www.quickfixengine.org/LICENSE for licensing information.
**
** Contact ask@quickfixengine.org if any conditions of this licensing are
** not clear to you.
**
****************************************************************************/

#ifdef _MSC_VER
#include "stdafx.h"
#else
#include "config.h"
#endif

#include "SocketConnector.h"
#include "Utility.h"
#ifndef _MSC_VER
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#include <iostream>

namespace FIX {
/// Handles events from SocketMonitor for client connections.
class ConnectorWrapper : public SocketMonitor::Strategy {
public:
  ConnectorWrapper(SocketConnector &connector, SocketConnector::Strategy &strategy)
      : m_connector(connector),
        m_strategy(strategy) {}

private:
  virtual void onConnect(SocketMonitor &, socket_handle socket) override { m_strategy.onConnect(m_connector, socket); }

  virtual void onWrite(SocketMonitor &, socket_handle socket) override { m_strategy.onWrite(m_connector, socket); }

  virtual void onEvent(SocketMonitor &, socket_handle socket) override {
    if (!m_strategy.onData(m_connector, socket)) {
      m_strategy.onDisconnect(m_connector, socket);
    }
  }

  virtual void onError(SocketMonitor &monitor, socket_handle socket) override {
    if (monitor.drop(socket)) {
      m_strategy.onDisconnect(m_connector, socket);
    }
  }

  virtual void onError(SocketMonitor &) override { m_strategy.onError(m_connector); }

  virtual void onTimeout(SocketMonitor &) override { m_strategy.onTimeout(m_connector); };

  SocketConnector &m_connector;
  SocketConnector::Strategy &m_strategy;
};

SocketConnector::SocketConnector(int timeout)
    : m_monitor(timeout) {}

socket_handle SocketConnector::connect(
    const std::string &address,
    int port,
    bool noDelay,
    int sendBufSize,
    int rcvBufSize,
    const std::string &sourceAddress,
    int sourcePort) {
  m_lastConnectError.clear();

  socket_handle socket = socket_createConnector();

  if (socket != INVALID_SOCKET_HANDLE) {
    if (noDelay) {
      socket_setsockopt(socket, TCP_NODELAY);
    }
    if (sendBufSize) {
      socket_setsockopt(socket, SO_SNDBUF, sendBufSize);
    }
    if (rcvBufSize) {
      socket_setsockopt(socket, SO_RCVBUF, rcvBufSize);
    }
    if (!sourceAddress.empty() || sourcePort) {
      socket_bind(socket, sourceAddress.c_str(), sourcePort);
    }

    // Connect with the socket already non-blocking so a failed attempt never
    // stalls the reactor thread; only connects that are still in progress (or
    // already established) are handed to the monitor.
    socket_setnonblock(socket);
#ifndef _MSC_VER
    // socket_connect returns without setting errno when the host is unknown,
    // so clear it to avoid acting on a stale EINPROGRESS from an earlier call.
    errno = 0;
#endif
    if (socket_connect(socket, address.c_str(), port) != 0) {
#ifdef _MSC_VER
      int error = WSAGetLastError();
      if (error != WSAEWOULDBLOCK) {
        m_lastConnectError = error_wsaerror(error);
        socket_close(socket);
        return INVALID_SOCKET_HANDLE;
      }
#else
      if (errno != EINPROGRESS) {
        m_lastConnectError = error_strerror(errno);
        socket_close(socket);
        return INVALID_SOCKET_HANDLE;
      }
#endif
    }
    m_monitor.addConnect(socket);
  }
  return socket;
}

socket_handle SocketConnector::connect(
    const std::string &address,
    int port,
    bool noDelay,
    int sendBufSize,
    int rcvBufSize,
    Strategy &strategy) {
  socket_handle socket = connect(address, port, noDelay, sendBufSize, rcvBufSize, "", 0);
  return socket;
}

void SocketConnector::block(Strategy &strategy, bool poll, double timeout) {
  ConnectorWrapper wrapper(*this, strategy);
  m_monitor.block(wrapper, poll, timeout);
}
} // namespace FIX

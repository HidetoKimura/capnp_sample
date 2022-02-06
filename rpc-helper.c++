// Copyright (c) 2013-2014 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "rpc-helper.h"
#include <capnp/rpc-twoparty.h>
#include <capnp/rpc.capnp.h>
#include <kj/async-io.h>
#include <kj/debug.h>
#include <kj/threadlocal.h>
#include <map>

KJ_THREADLOCAL_PTR(RpcHelperContext) threadEzContext = nullptr;

class RpcHelperContext: public kj::Refcounted {
public:
  RpcHelperContext(): ioContext(kj::setupAsyncIo()) {
    threadEzContext = this;
  }

  ~RpcHelperContext() noexcept(false) {
    KJ_REQUIRE(threadEzContext == this,
               "RpcHelperContext destroyed from different thread than it was created.") {
      return;
    }
    threadEzContext = nullptr;
  }

  kj::WaitScope& getWaitScope() {
    return ioContext.waitScope;
  }

  kj::AsyncIoProvider& getIoProvider() {
    return *ioContext.provider;
  }

  kj::LowLevelAsyncIoProvider& getLowLevelIoProvider() {
    return *ioContext.lowLevelProvider;
  }

  kj::UnixEventPort& getUnixEventPort() {
    return ioContext.unixEventPort;
  }

  static kj::Own<RpcHelperContext> getThreadLocal() {
    RpcHelperContext* existing = threadEzContext;
    if (existing != nullptr) {
      return kj::addRef(*existing);
    } else {
      return kj::refcounted<RpcHelperContext>();
    }
  }

private:
  kj::AsyncIoContext ioContext;
};

// =======================================================================================

kj::Promise<kj::Own<kj::AsyncIoStream>> connectAttach(kj::Own<kj::NetworkAddress>&& addr) {
  return addr->connect().attach(kj::mv(addr));
}

struct RpcHelperClient::Impl {
  kj::Own<RpcHelperContext> context;

  struct ClientContext {
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;

    ClientContext(kj::Own<kj::AsyncIoStream>&& stream, capnp::ReaderOptions readerOpts)
        : stream(kj::mv(stream)),
          network(*this->stream, capnp::rpc::twoparty::Side::CLIENT, readerOpts),
          rpcSystem(makeRpcClient(network)) {}

    capnp::Capability::Client getMain() {
      capnp::word scratch[4];
      memset(scratch, 0, sizeof(scratch));
      capnp::MallocMessageBuilder message(scratch);
      auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
      hostId.setSide(capnp::rpc::twoparty::Side::SERVER);
      return rpcSystem.bootstrap(hostId);
    }

    capnp::Capability::Client restore(kj::StringPtr name) {
      capnp::word scratch[64];
      memset(scratch, 0, sizeof(scratch));
      capnp::MallocMessageBuilder message(scratch);

      auto hostIdOrphan = message.getOrphanage().newOrphan<capnp::rpc::twoparty::VatId>();
      auto hostId = hostIdOrphan.get();
      hostId.setSide(capnp::rpc::twoparty::Side::SERVER);

      auto objectId = message.getRoot<capnp::AnyPointer>();
      objectId.setAs<capnp::Text>(name);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      return rpcSystem.restore(hostId, objectId);
#pragma GCC diagnostic pop
    }
  };

  kj::ForkedPromise<void> setupPromise;

  kj::Maybe<kj::Own<ClientContext>> clientContext;
  // Filled in before `setupPromise` resolves.

  Impl(kj::StringPtr serverAddress, uint defaultPort,
       capnp::ReaderOptions readerOpts)
      : context(RpcHelperContext::getThreadLocal()),
        setupPromise(context->getIoProvider().getNetwork()
            .parseAddress(serverAddress, defaultPort)
            .then([](kj::Own<kj::NetworkAddress>&& addr) {
              return connectAttach(kj::mv(addr));
            }).then([this, readerOpts](kj::Own<kj::AsyncIoStream>&& stream) {
              clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                                      readerOpts);
            }).fork()) {}

  Impl(const struct sockaddr* serverAddress, uint addrSize,
       capnp::ReaderOptions readerOpts)
      : context(RpcHelperContext::getThreadLocal()),
        setupPromise(
            connectAttach(context->getIoProvider().getNetwork()
                .getSockaddr(serverAddress, addrSize))
            .then([this, readerOpts](kj::Own<kj::AsyncIoStream>&& stream) {
              clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                                      readerOpts);
            }).fork()) {}

  Impl(int socketFd, capnp::ReaderOptions readerOpts)
      : context(RpcHelperContext::getThreadLocal()),
        setupPromise(kj::Promise<void>(kj::READY_NOW).fork()),
        clientContext(kj::heap<ClientContext>(
            context->getLowLevelIoProvider().wrapSocketFd(socketFd),
            readerOpts)) {}
};

RpcHelperClient::RpcHelperClient(kj::StringPtr serverAddress, uint defaultPort, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(serverAddress, defaultPort, readerOpts)) {}

RpcHelperClient::RpcHelperClient(const struct sockaddr* serverAddress, uint addrSize, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(serverAddress, addrSize, readerOpts)) {}

RpcHelperClient::RpcHelperClient(int socketFd, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(socketFd, readerOpts)) {}

RpcHelperClient::~RpcHelperClient() noexcept(false) {}

capnp::Capability::Client RpcHelperClient::getMain() {
  KJ_IF_MAYBE(client, impl->clientContext) {
    return client->get()->getMain();
  } else {
    return impl->setupPromise.addBranch().then([this]() {
      return KJ_ASSERT_NONNULL(impl->clientContext)->getMain();
    });
  }
}

capnp::Capability::Client RpcHelperClient::importCap(kj::StringPtr name) {
  KJ_IF_MAYBE(client, impl->clientContext) {
    return client->get()->restore(name);
  } else {
    return impl->setupPromise.addBranch().then(kj::mvCapture(kj::heapString(name),
        [this](kj::String&& name) {
      return KJ_ASSERT_NONNULL(impl->clientContext)->restore(name);
    }));
  }
}

kj::WaitScope& RpcHelperClient::getWaitScope() {
  return impl->context->getWaitScope();
}

kj::AsyncIoProvider& RpcHelperClient::getIoProvider() {
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& RpcHelperClient::getLowLevelIoProvider() {
  return impl->context->getLowLevelIoProvider();
}

kj::UnixEventPort& RpcHelperClient::getUnixEventPort() {
  return impl->context->getUnixEventPort();
}


// =======================================================================================

namespace {

class DummyFilter: public kj::LowLevelAsyncIoProvider::NetworkFilter {
public:
  bool shouldAllow(const struct sockaddr* addr, uint addrlen) override {
    return true;
  }
};

static DummyFilter DUMMY_FILTER;

}  // namespace

struct RpcHelperServer::Impl final: public capnp::SturdyRefRestorer<capnp::AnyPointer>,
                                public kj::TaskSet::ErrorHandler {
  capnp::Capability::Client mainInterface;
  kj::Own<RpcHelperContext> context;

  struct ExportedCap {
    kj::String name;
    capnp::Capability::Client cap = nullptr;

    ExportedCap(kj::StringPtr name, capnp::Capability::Client cap)
        : name(kj::heapString(name)), cap(cap) {}

    ExportedCap() = default;
    ExportedCap(const ExportedCap&) = delete;
    ExportedCap(ExportedCap&&) = default;
    ExportedCap& operator=(const ExportedCap&) = delete;
    ExportedCap& operator=(ExportedCap&&) = default;
    // Make std::map happy...
  };

  std::map<kj::StringPtr, ExportedCap> exportMap;

  kj::ForkedPromise<uint> portPromise;

  kj::TaskSet tasks;

  struct ServerContext {
    kj::Own<kj::AsyncIoStream> stream;
    capnp::TwoPartyVatNetwork network;
    capnp::RpcSystem<capnp::rpc::twoparty::VatId> rpcSystem;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    ServerContext(kj::Own<kj::AsyncIoStream>&& stream, SturdyRefRestorer<capnp::AnyPointer>& restorer,
                  capnp::ReaderOptions readerOpts)
        : stream(kj::mv(stream)),
          network(*this->stream, capnp::rpc::twoparty::Side::SERVER, readerOpts),
          rpcSystem(makeRpcServer(network, restorer)) {}
#pragma GCC diagnostic pop
  };

  Impl(capnp::Capability::Client mainInterface, kj::StringPtr bindAddress, uint defaultPort,
       capnp::ReaderOptions readerOpts)
      : mainInterface(kj::mv(mainInterface)),
        context(RpcHelperContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    auto paf = kj::newPromiseAndFulfiller<uint>();
    portPromise = paf.promise.fork();

    tasks.add(context->getIoProvider().getNetwork().parseAddress(bindAddress, defaultPort)
        .then(kj::mvCapture(paf.fulfiller,
          [this, readerOpts](kj::Own<kj::PromiseFulfiller<uint>>&& portFulfiller,
                             kj::Own<kj::NetworkAddress>&& addr) {
      auto listener = addr->listen();
      portFulfiller->fulfill(listener->getPort());
      acceptLoop(kj::mv(listener), readerOpts);
    })));
  }

  Impl(capnp::Capability::Client mainInterface, struct sockaddr* bindAddress, uint addrSize,
       capnp::ReaderOptions readerOpts)
      : mainInterface(kj::mv(mainInterface)),
        context(RpcHelperContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    auto listener = context->getIoProvider().getNetwork()
        .getSockaddr(bindAddress, addrSize)->listen();
    portPromise = kj::Promise<uint>(listener->getPort()).fork();
    acceptLoop(kj::mv(listener), readerOpts);
  }

  Impl(capnp::Capability::Client mainInterface, int socketFd, uint port, capnp::ReaderOptions readerOpts)
      : mainInterface(kj::mv(mainInterface)),
        context(RpcHelperContext::getThreadLocal()),
        portPromise(kj::Promise<uint>(port).fork()),
        tasks(*this) {
    acceptLoop(context->getLowLevelIoProvider().wrapListenSocketFd(socketFd, DUMMY_FILTER),
               readerOpts);
  }

  void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, capnp::ReaderOptions readerOpts) {
    auto ptr = listener.get();
    tasks.add(ptr->accept().then(kj::mvCapture(kj::mv(listener),
        [this, readerOpts](kj::Own<kj::ConnectionReceiver>&& listener,
                           kj::Own<kj::AsyncIoStream>&& connection) {
      acceptLoop(kj::mv(listener), readerOpts);

      auto server = kj::heap<ServerContext>(kj::mv(connection), *this, readerOpts);

      // Arrange to destroy the server context when all references are gone, or when the
      // RpcHelperServer is destroyed (which will destroy the TaskSet).
      tasks.add(server->network.onDisconnect().attach(kj::mv(server)));
    })));
  }

  capnp::Capability::Client restore(capnp::AnyPointer::Reader objectId) override {
    if (objectId.isNull()) {
      return mainInterface;
    } else {
      auto name = objectId.getAs<capnp::Text>();
      auto iter = exportMap.find(name);
      if (iter == exportMap.end()) {
        KJ_FAIL_REQUIRE("Server exports no such capability.", name) { break; }
        return nullptr;
      } else {
        return iter->second.cap;
      }
    }
  }

  void taskFailed(kj::Exception&& exception) override {
    kj::throwFatalException(kj::mv(exception));
  }
};

RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, kj::StringPtr bindAddress,
                         uint defaultPort, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, defaultPort, readerOpts)) {}

RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, struct sockaddr* bindAddress,
                         uint addrSize, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, addrSize, readerOpts)) {}

RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, int socketFd, uint port,
                         capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), socketFd, port, readerOpts)) {}

RpcHelperServer::RpcHelperServer(kj::StringPtr bindAddress, uint defaultPort,
                         capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, bindAddress, defaultPort, readerOpts) {}

RpcHelperServer::RpcHelperServer(struct sockaddr* bindAddress, uint addrSize,
                         capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, bindAddress, addrSize, readerOpts) {}

RpcHelperServer::RpcHelperServer(int socketFd, uint port, capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, socketFd, port, readerOpts) {}

RpcHelperServer::~RpcHelperServer() noexcept(false) {}

void RpcHelperServer::exportCap(kj::StringPtr name, capnp::Capability::Client cap) {
  Impl::ExportedCap entry(kj::heapString(name), cap);
  impl->exportMap[entry.name] = kj::mv(entry);
}

kj::Promise<uint> RpcHelperServer::getPort() {
  return impl->portPromise.addBranch();
}

kj::WaitScope& RpcHelperServer::getWaitScope() {
  return impl->context->getWaitScope();
}

kj::AsyncIoProvider& RpcHelperServer::getIoProvider() {
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& RpcHelperServer::getLowLevelIoProvider() {
  return impl->context->getLowLevelIoProvider();
}

kj::UnixEventPort& RpcHelperServer::getUnixEventPort() {
  return impl->context->getUnixEventPort();
}


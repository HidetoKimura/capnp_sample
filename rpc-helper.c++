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
#include <sys/socket.h>

KJ_THREADLOCAL_PTR(RpcHelperContext) threadEzContext = nullptr;

class RpcHelperContext: public kj::Refcounted {
public:
  RpcHelperContext(): ioContext(kj::setupAsyncIo()) {
    threadEzContext = this;
    KJ_TRC();
  }

  ~RpcHelperContext() noexcept(false) {
    KJ_REQUIRE(threadEzContext == this,
               "RpcHelperContext destroyed from different thread than it was created.") {
      return;
    }
    threadEzContext = nullptr;
    KJ_TRC();
  }

  kj::WaitScope& getWaitScope() {
    KJ_TRC();
    return ioContext.waitScope;
  }

  kj::AsyncIoProvider& getIoProvider() {
    KJ_TRC();
    return *ioContext.provider;
  }

  kj::LowLevelAsyncIoProvider& getLowLevelIoProvider() {
    KJ_TRC();
    return *ioContext.lowLevelProvider;
  }

  kj::UnixEventPort& getUnixEventPort() {
    KJ_TRC();
    return ioContext.unixEventPort;
  }

  static kj::Own<RpcHelperContext> getThreadLocal() {
    KJ_TRC();
    RpcHelperContext* existing = threadEzContext;
    if (existing != nullptr) {
      KJ_TRC();
      return kj::addRef(*existing);
    } else {
      KJ_TRC();
      return kj::refcounted<RpcHelperContext>();
    }
  }

private:
  kj::AsyncIoContext ioContext;
};

// =======================================================================================

kj::Promise<kj::Own<kj::AsyncIoStream>> connectAttach(kj::Own<kj::NetworkAddress>&& addr) {
  KJ_TRC();
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
          rpcSystem(makeRpcClient(network))
    {
      KJ_TRC();
    }

#if 0
        : stream(kj::mv(stream)),
          network(*this->stream, capnp::rpc::twoparty::Side::CLIENT, readerOpts),
          rpcSystem(makeRpcClient(network)) {}
#endif

    capnp::Capability::Client getMain() {
      KJ_TRC();
      capnp::word scratch[4];
      memset(scratch, 0, sizeof(scratch));
      capnp::MallocMessageBuilder message(scratch);
      auto hostId = message.getRoot<capnp::rpc::twoparty::VatId>();
      KJ_TRC("after getRoot");
      hostId.setSide(capnp::rpc::twoparty::Side::SERVER);
      KJ_TRC("after setSide");
      capnp::Capability::Client client = rpcSystem.bootstrap(hostId);
      KJ_TRC("after bootstrap");
      return client;
    }

    capnp::Capability::Client restore(kj::StringPtr name) {
      KJ_TRC();
      capnp::word scratch[64];
      memset(scratch, 0, sizeof(scratch));
      capnp::MallocMessageBuilder message(scratch);

      KJ_TRC();
      auto hostIdOrphan = message.getOrphanage().newOrphan<capnp::rpc::twoparty::VatId>();
      auto hostId = hostIdOrphan.get();
      hostId.setSide(capnp::rpc::twoparty::Side::SERVER);
      KJ_TRC();

      auto objectId = message.getRoot<capnp::AnyPointer>();

      KJ_TRC();
      objectId.setAs<capnp::Text>(name);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      KJ_TRC();
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
              KJ_TRC(*addr);
              return connectAttach(kj::mv(addr));
            }).then([this, readerOpts](kj::Own<kj::AsyncIoStream>&& stream) {
              KJ_TRC();
              clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                                      readerOpts);
            }).fork()) {
              KJ_TRC();
            }

  Impl(const struct sockaddr* serverAddress, uint addrSize,
       capnp::ReaderOptions readerOpts)
      : context(RpcHelperContext::getThreadLocal()),
        setupPromise(
            connectAttach(context->getIoProvider().getNetwork()
                .getSockaddr(serverAddress, addrSize))
            .then([this, readerOpts](kj::Own<kj::AsyncIoStream>&& stream) {
              clientContext = kj::heap<ClientContext>(kj::mv(stream),
                                                      readerOpts);
            }).fork()) {
              KJ_TRC();
            }

  Impl(int socketFd, capnp::ReaderOptions readerOpts)
      : context(RpcHelperContext::getThreadLocal()),
        setupPromise(kj::Promise<void>(kj::READY_NOW).fork()),
        clientContext(kj::heap<ClientContext>(
            context->getLowLevelIoProvider().wrapSocketFd(socketFd),
            readerOpts)) {
              KJ_TRC();
            }
};

RpcHelperClient::RpcHelperClient(kj::StringPtr serverAddress, uint defaultPort, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(serverAddress, defaultPort, readerOpts)) {
      KJ_TRC();
    }

RpcHelperClient::RpcHelperClient(const struct sockaddr* serverAddress, uint addrSize, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(serverAddress, addrSize, readerOpts)) {
      KJ_TRC();
    }

RpcHelperClient::RpcHelperClient(int socketFd, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(socketFd, readerOpts)) {
      KJ_TRC();
    }

RpcHelperClient::~RpcHelperClient() noexcept(false) {
      KJ_TRC();
}

capnp::Capability::Client RpcHelperClient::getMain() {
  KJ_IF_MAYBE(client, impl->clientContext) {
    KJ_TRC();
    return client->get()->getMain();
  } else {
    KJ_TRC();
    return impl->setupPromise.addBranch().then([this]() {
      KJ_TRC();
      return KJ_ASSERT_NONNULL(impl->clientContext)->getMain();
    });
  }
}

capnp::Capability::Client RpcHelperClient::importCap(kj::StringPtr name) {
  KJ_IF_MAYBE(client, impl->clientContext) {
    KJ_TRC();
    return client->get()->restore(name);
  } else {
    return impl->setupPromise.addBranch().then(kj::mvCapture(kj::heapString(name),
        [this](kj::String&& name) {
      KJ_TRC();
      return KJ_ASSERT_NONNULL(impl->clientContext)->restore(name);
    }));
  }
}

kj::WaitScope& RpcHelperClient::getWaitScope() {
  return impl->context->getWaitScope();
}

kj::AsyncIoProvider& RpcHelperClient::getIoProvider() {
  KJ_TRC();
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& RpcHelperClient::getLowLevelIoProvider() {
  KJ_TRC();
  return impl->context->getLowLevelIoProvider();
}

kj::UnixEventPort& RpcHelperClient::getUnixEventPort() {
  KJ_TRC();
  return impl->context->getUnixEventPort();
}


// =======================================================================================

namespace {

class DummyFilter: public kj::LowLevelAsyncIoProvider::NetworkFilter {
public:
  bool shouldAllow(const struct sockaddr* addr, uint addrlen) override {
    KJ_TRC();
    return true;
  }
};

static DummyFilter DUMMY_FILTER;

}  // namespace

struct RpcHelperServer::Impl final: public capnp::SturdyRefRestorer<capnp::AnyPointer>,
                                public kj::TaskSet::ErrorHandler {
  capnp::Capability::Client mainInterface;
//  std::vector<capnp::Capability::Client> mainInterfaces
  kj::Own<RpcHelperContext> context;

  struct ExportedCap {
    kj::String name;
    capnp::Capability::Client cap = nullptr;

    ExportedCap(kj::StringPtr name, capnp::Capability::Client cap)
        : name(kj::heapString(name)), cap(cap) {
          KJ_TRC();          
        }

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
          rpcSystem(makeRpcServer(network, restorer)) {
            KJ_TRC();                      
          }
#pragma GCC diagnostic pop
  };
#if 0
  Impl(std::vector<capnp::Capability::Client> mainInterfaces, kj::StringPtr bindAddress, uint defaultPort,
       capnp::ReaderOptions readerOpts)
      : mainInterfaces(std::move(mainInterfaces)),
        context(RpcHelperContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
#endif
  Impl(capnp::Capability::Client mainInterface, kj::StringPtr bindAddress, uint defaultPort,
       capnp::ReaderOptions readerOpts)
      : mainInterface(kj::mv(mainInterface)),
        context(RpcHelperContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    KJ_TRC();          
    auto paf = kj::newPromiseAndFulfiller<uint>();
    portPromise = paf.promise.fork();

    tasks.add(context->getIoProvider().getNetwork().parseAddress(bindAddress, defaultPort)
        .then(kj::mvCapture(paf.fulfiller,
          [this, readerOpts](kj::Own<kj::PromiseFulfiller<uint>>&& portFulfiller,
                             kj::Own<kj::NetworkAddress>&& addr) {
      KJ_TRC();          
      auto listener = addr->listen();
      portFulfiller->fulfill(listener->getPort());
      acceptLoop(kj::mv(listener), readerOpts);
    })));
  }

  Impl(capnp::Capability::Client mainInterface, struct sockaddr* bindAddress, uint addrSize,
       capnp::ReaderOptions readerOpts)
      : mainInterface(kj::mv(mainInterface)),
        context(RpcHelperContext::getThreadLocal()), portPromise(nullptr), tasks(*this) {
    KJ_TRC();          
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
    KJ_TRC();          
    acceptLoop(context->getLowLevelIoProvider().wrapListenSocketFd(socketFd, DUMMY_FILTER),
               readerOpts);
  }

  void acceptLoop(kj::Own<kj::ConnectionReceiver>&& listener, capnp::ReaderOptions readerOpts) {
    KJ_TRC();          
    auto ptr = listener.get();
    tasks.add(ptr->accept().then(kj::mvCapture(kj::mv(listener),
        [this, readerOpts](kj::Own<kj::ConnectionReceiver>&& listener,
                           kj::Own<kj::AsyncIoStream>&& connection) {
      KJ_TRC();          
      acceptLoop(kj::mv(listener), readerOpts);

      auto server = kj::heap<ServerContext>(kj::mv(connection), *this, readerOpts);

      // Arrange to destroy the server context when all references are gone, or when the
      // RpcHelperServer is destroyed (which will destroy the TaskSet).
      tasks.add(server->network.onDisconnect().attach(kj::mv(server)));
    })));
  }

  capnp::Capability::Client restore(capnp::AnyPointer::Reader objectId) override {
    KJ_MES("restore");          
    if (objectId.isNull()) {
      KJ_MES(objectId.isNull());          
      return mainInterface;
    } else {
      KJ_MES();          
      auto name = objectId.getAs<capnp::Text>();
      auto iter = exportMap.find(name);
      if (iter == exportMap.end()) {
        KJ_FAIL_REQUIRE("Server exports no such capability.", name) { break; }
        return nullptr;
      } else {
        KJ_MES();          
        return iter->second.cap;
      }
    }
  }

  void taskFailed(kj::Exception&& exception) override {
    KJ_TRC();          
    kj::throwFatalException(kj::mv(exception));
  }
};

#if 0
RpcHelperServer::RpcHelperServer(std::vector<capnp::Capability::Client> mainInterfaces, kj::StringPtr bindAddress,
                         uint defaultPort, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(std::move(mainInterfaces), bindAddress, defaultPort, readerOpts)) {
#endif
RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, kj::StringPtr bindAddress,
                         uint defaultPort, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, defaultPort, readerOpts)) {
      KJ_TRC();          
    }

RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, struct sockaddr* bindAddress,
                         uint addrSize, capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), bindAddress, addrSize, readerOpts)) {
      KJ_TRC();          
    }

RpcHelperServer::RpcHelperServer(capnp::Capability::Client mainInterface, int socketFd, uint port,
                         capnp::ReaderOptions readerOpts)
    : impl(kj::heap<Impl>(kj::mv(mainInterface), socketFd, port, readerOpts)) {
      KJ_TRC();          
    }

RpcHelperServer::RpcHelperServer(kj::StringPtr bindAddress, uint defaultPort,
                         capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, bindAddress, defaultPort, readerOpts) {
      KJ_TRC();          
    }

RpcHelperServer::RpcHelperServer(struct sockaddr* bindAddress, uint addrSize,
                         capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, bindAddress, addrSize, readerOpts) {
      KJ_TRC();          
    }

RpcHelperServer::RpcHelperServer(int socketFd, uint port, capnp::ReaderOptions readerOpts)
    : RpcHelperServer(nullptr, socketFd, port, readerOpts) {
      KJ_TRC();          
    }

RpcHelperServer::~RpcHelperServer() noexcept(false) {
  KJ_TRC();          
}

void RpcHelperServer::exportCap(kj::StringPtr name, capnp::Capability::Client cap) {
  Impl::ExportedCap entry(kj::heapString(name), cap);
  impl->exportMap[entry.name] = kj::mv(entry);
  KJ_TRC();          
}

kj::Promise<uint> RpcHelperServer::getPort() {
  KJ_TRC();          
  return impl->portPromise.addBranch();
}

kj::WaitScope& RpcHelperServer::getWaitScope() {
  KJ_TRC();          
  return impl->context->getWaitScope();
}

kj::AsyncIoProvider& RpcHelperServer::getIoProvider() {
  KJ_TRC();          
  return impl->context->getIoProvider();
}

kj::LowLevelAsyncIoProvider& RpcHelperServer::getLowLevelIoProvider() {
  KJ_TRC();          
  return impl->context->getLowLevelIoProvider();
}

kj::UnixEventPort& RpcHelperServer::getUnixEventPort() {
  KJ_TRC();          
  return impl->context->getUnixEventPort();
}


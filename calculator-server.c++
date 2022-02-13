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
#include "calculator.capnp.h"
#include <kj/debug.h>
#include <kj/async-unix.h>
#include <capnp/message.h>
#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

typedef unsigned int uint;
void promise_test(kj::WaitScope& waitScope);

kj::Promise<double> readValue(Calculator::Value::Client value) {
  // Helper function to asynchronously call read() on a Calculator::Value and
  // return a promise for the result.  (In the future, the generated code might
  // include something like this automatically.)

  return value.readRequest().send()
      .then([](capnp::Response<Calculator::Value::ReadResults> result) {
    return result.getValue();
  });
}

kj::Promise<double> evaluateImpl(
    Calculator::Expression::Reader expression,
    capnp::List<double>::Reader params = capnp::List<double>::Reader()) {
  // Implementation of CalculatorImpl::evaluate(), also shared by
  // FunctionImpl::call().  In the latter case, `params` are the parameter
  // values passed to the function; in the former case, `params` is just an
  // empty list.

  switch (expression.which()) {
    case Calculator::Expression::LITERAL:
      return expression.getLiteral();

    case Calculator::Expression::PREVIOUS_RESULT:
      return readValue(expression.getPreviousResult());

    case Calculator::Expression::PARAMETER: {
      KJ_REQUIRE(expression.getParameter() < params.size(),
                 "Parameter index out-of-range.");
      return params[expression.getParameter()];
    }

    case Calculator::Expression::CALL: {
      auto call = expression.getCall();
      auto func = call.getFunction();

      // Evaluate each parameter.
      kj::Array<kj::Promise<double>> paramPromises =
          KJ_MAP(param, call.getParams()) {
            return evaluateImpl(param, params);
          };

      // Join the array of promises into a promise for an array.
      kj::Promise<kj::Array<double>> joinedParams =
          kj::joinPromises(kj::mv(paramPromises));

      // When the parameters are complete, call the function.
      return joinedParams.then([KJ_CPCAP(func)](kj::Array<double>&& paramValues) mutable {
        auto request = func.callRequest();
        request.setParams(paramValues);
        return request.send().then(
            [](capnp::Response<Calculator::Function::CallResults>&& result) {
          return result.getValue();
        });
      });
    }

    default:
      // Throw an exception.
      KJ_FAIL_REQUIRE("Unknown expression type.");
  }
}

class ValueImpl final: public Calculator::Value::Server {
  // Simple implementation of the Calculator.Value Cap'n Proto interface.

public:
  ValueImpl(double value): value(value) {}

  kj::Promise<void> read(ReadContext context) {
    context.getResults().setValue(value);
    return kj::READY_NOW;
  }

private:
  double value;
};

class FunctionImpl final: public Calculator::Function::Server {
  // Implementation of the Calculator.Function Cap'n Proto interface, where the
  // function is defined by a Calculator.Expression.

public:
  FunctionImpl(uint paramCount, Calculator::Expression::Reader body)
      : paramCount(paramCount) {
    this->body.setRoot(body);
  }

  kj::Promise<void> call(CallContext context) {
    auto params = context.getParams().getParams();
    KJ_REQUIRE(params.size() == paramCount, "Wrong number of parameters.");

    return evaluateImpl(body.getRoot<Calculator::Expression>(), params)
        .then([KJ_CPCAP(context)](double value) mutable {
      context.getResults().setValue(value);
    });
  }

private:
  uint paramCount;
  // The function's arity.

  capnp::MallocMessageBuilder body;
  // Stores a permanent copy of the function body.
};

class OperatorImpl final: public Calculator::Function::Server {
  // Implementation of the Calculator.Function Cap'n Proto interface, wrapping
  // basic binary arithmetic operators.

public:
  OperatorImpl(Calculator::Operator op): op(op) {}

  kj::Promise<void> call(CallContext context) {
    auto params = context.getParams().getParams();
    KJ_REQUIRE(params.size() == 2, "Wrong number of parameters.");

    double result;
    switch (op) {
      case Calculator::Operator::ADD:     result = params[0] + params[1]; break;
      case Calculator::Operator::SUBTRACT:result = params[0] - params[1]; break;
      case Calculator::Operator::MULTIPLY:result = params[0] * params[1]; break;
      case Calculator::Operator::DIVIDE:  result = params[0] / params[1]; break;
      default:
        KJ_FAIL_REQUIRE("Unknown operator.");
    }

    context.getResults().setValue(result);
    return kj::READY_NOW;
  }

private:
  Calculator::Operator op;
};

class CalculatorImpl final: public Calculator::Server {
  // Implementation of the Calculator Cap'n Proto interface.
public:
  CalculatorImpl(int id) : id_(id) {}
  kj::Promise<void> evaluate(EvaluateContext context) override {
    KJ_DBG(id_);
    return evaluateImpl(context.getParams().getExpression())
        .then([KJ_CPCAP(context)](double value) mutable {
      context.getResults().setValue(kj::heap<ValueImpl>(value));
    });
  }

  kj::Promise<void> defFunction(DefFunctionContext context) override {
    auto params = context.getParams();
    KJ_DBG(id_);
    context.getResults().setFunc(kj::heap<FunctionImpl>(
        params.getParamCount(), params.getBody()));
    return kj::READY_NOW;
  }

  kj::Promise<void> getOperator(GetOperatorContext context) override {
    KJ_DBG(id_);
    context.getResults().setFunc(kj::heap<OperatorImpl>(
        context.getParams().getOp()));
    return kj::READY_NOW;
  }
private:
  int id_;
};

int main(int argc, const char* argv[]) {
  if (argc != 2) {
    std::cerr << "usage: " << argv[0] << " ADDRESS[:PORT]\n"
        "Runs the server bound to the given address/port.\n"
        "ADDRESS may be '*' to bind to all local addresses.\n"
        ":PORT may be omitted to choose a port automatically." << std::endl;
    return 1;
  }
  
  std::string endpoint(argv[1]);
  std::string word("unix:");
  auto pos = endpoint.find(word);
  if(pos != std::string::npos) {
    auto path = endpoint.substr(pos + word.size()).c_str();
    unlink(path);
  }

  #if 0
  KJ_TRC();
  std::vector<capnp::Capability::Client> impl;
  impl.push_back(kj::heap<CalculatorImpl>(1));
  impl.push_back(kj::heap<CalculatorImpl>(2));
  #endif

  // Set up a server.
  RpcHelperServer server(kj::heap<CalculatorImpl>(0), argv[1]);
  server.exportCap("CAP1", kj::heap<CalculatorImpl>(123));
  server.exportCap("CAP2", kj::heap<CalculatorImpl>(456));

  KJ_TRC();

  // Write the port number to stdout, in case it was chosen automatically.
  auto& waitScope = server.getWaitScope();
  uint port = server.getPort().wait(waitScope);
  if (port == 0) {
    // The address format "unix:/path/to/socket" opens a unix domain socket,
    // in which case the port will be zero.
    std::cout << "Listening on Unix socket..." << endpoint << std::endl;
  } else {
    std::cout << "Listening on port " << port << "..." << endpoint << std::endl;
  }

  int sock_;
  KJ_SYSCALL(sock_ = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0));
  auto sock = kj::AutoCloseFd(sock_);

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, "/tmp/sample-server.sock");
  unlink(addr.sun_path);
  KJ_SYSCALL(bind(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)));

  auto& ev_port = server.getUnixEventPort();
  kj::UnixEventPort::FdObserver observer(ev_port, sock, kj::UnixEventPort::FdObserver::OBSERVE_READ);
  
  promise_test(waitScope);

  for (;;) {
    kj::Promise<void> watcher = observer.whenBecomesReadable();
    KJ_DBG(watcher.trace());
    watcher.then([&](){
      char  data[256];
      int32_t len;
      KJ_SYSCALL(len = read(sock ,data,sizeof(data)));
      if(len < sizeof(data)) {
        data[len] = '\0';
      }
      std::cout << "len = " << len << ", read data = " << data << std::endl;
    }).wait(waitScope);
  }

}

void promise_test(kj::WaitScope& waitScope)
{

  kj::Promise<int> test1 = 123;
  kj::Promise<int> test2 = test1.then([](int i){return 456;});
  kj::Promise<int> test3 = test2.then([](int i){return 789;});

  KJ_DBG(test3.trace());
  KJ_DBG(test3.wait(waitScope));

  kj::Promise<int> promise = kj::evalLater([&]() { return 123; });

  auto fork = promise.fork();

  KJ_ASSERT(!fork.hasBranches());
  {
    auto cancelBranch = fork.addBranch();
    KJ_ASSERT(fork.hasBranches());
  }
  KJ_ASSERT(!fork.hasBranches());

  auto branch1 = fork.addBranch().then([](int i) {
    if(i == 123) {
      return 456;
    }
    else {
      return 0;
    }
  });
  KJ_ASSERT(fork.hasBranches());

  auto branch2 = fork.addBranch().then([](int i) {
    if(i == 123) {
      return 789;
    }
    else {
      return 0;
    }
  });
  KJ_ASSERT(fork.hasBranches());

  {
    auto releaseFork = kj::mv(fork);
  }

  KJ_DBG(branch1.wait(waitScope));
  KJ_DBG(branch2.wait(waitScope));

  return;
}
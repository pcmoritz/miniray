#pragma once

#include <deque>
#include <memory>
#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"

#include "util.h"

using ActorExecutor = std::function<std::string(void**, const std::string&, const std::string&, bool* error_happened)>;

class Future;

struct Task {
  std::string method_name;
  // Pickled collection of (args, kwargs)
  std::string args_data;
  // Result of the computation
  std::shared_ptr<Future> result;
};

class Actor {
public:
  void Start(ActorExecutor executor);
  std::shared_ptr<Future> Submit(std::string& method_name, std::string& arg_data);
  ~Actor();

private:
  bool ExecuteTasks(const ActorExecutor& executor);

  // Mutex to protect the state of this actor.
  mutable absl::Mutex mu_;
  // Storage that the language implementation can use to store
  // state correspoinding to the actor (e.g. the PyObject* of
  // the object backing the actor).
  void* actor_ GUARDED_BY(mu_);
  // Thread running this actor.
  std::thread thread_ GUARDED_BY(mu_);
  // Task queue for this actor.
  std::deque<Task> queue_ GUARDED_BY(mu_);
};

// Proxy for a result from an actor call. These are typically
// passed around in user code as std::shared_ptr<Future>
// smart pointers, which allows us to do reference counting
// and deallocate the underlying memory for the result once the
// future is not reachable any more.
class Future {
public:
  Future() { mu_.Lock(); }
  std::string data() {
    FastMutexLock lock(&mu_);
    return data_;
  }
  void set_ready(std::string value) {
    mu_.AssertHeld();
    data_ = std::move(value);
    mu_.Unlock();
  }
private:
  // This mutex is locked while the future is not yet ready
  mutable absl::Mutex mu_;
  // Data for the result of this future after it is ready
  std::string data_ GUARDED_BY(mu_);
};

class Context {
public:
  Context() {};
  std::shared_ptr<Actor> MakeActor(ActorExecutor executor, std::string init_args_data);
  std::string Get(const std::shared_ptr<Future>& future);
};
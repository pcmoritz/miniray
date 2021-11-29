#pragma once

#include <deque>
#include <memory>
#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"

class Actor;
class Future;

struct SerializedObject {
  std::string data;
  std::vector<std::shared_ptr<Actor>> handles;
  bool error = false;
};

using ActorExecutor = std::function<SerializedObject(void*, const std::string&, SerializedObject)>;

struct Task {
  std::string method_name;
  // Serialized object of arguments
  SerializedObject serialized_args;
  // Result of the computation
  std::shared_ptr<Future> result;
};

class Actor {
public:
  Actor() {};
  void Start(void* actor, ActorExecutor executor);
  std::shared_ptr<Future> Submit(std::string& method_name, SerializedObject serialized_args);
  ~Actor();

private:
  bool ExecuteTasks(const ActorExecutor& executor);

  // Mutex to protect the state of this actor
  mutable absl::Mutex mu_;
  // Storage that the language implementation can use to store
  // state correspoinding to the actor (e.g. the PyObject* of
  // the object backing the actor)
  void* actor_;
  // Thread running this actor
  std::thread thread_ GUARDED_BY(mu_);
  // Task queue for this actor
  std::deque<Task> queue_ GUARDED_BY(mu_);
  // Possible error returned by actor's constructor
  SerializedObject init_result_;
};

// Proxy for a result from an actor call. These are typically
// passed around in user code as std::shared_ptr<Future>
// smart pointers, which allows us to do reference counting
// and deallocate the underlying memory for the result once the
// future is not reachable any more.
class Future {
public:
  explicit Future() : ready_(false) {}
  SerializedObject data() {
    absl::MutexLock lock(&mu_, absl::Condition(&ready_));
    return data_;
  }
  void set_ready(SerializedObject value) {
    absl::MutexLock lock(&mu_);
    data_ = value;
    ready_ = true;
  }
  ~Future();
private:
  // Protect the state of this future
  mutable absl::Mutex mu_;
  // Indicate whether the future is ready
  bool ready_ GUARDED_BY(mu_);
  // Data for the result of this future after it is ready
  SerializedObject data_ GUARDED_BY(mu_);
};

class Context {
public:
  std::shared_ptr<Actor> MakeActor(void* actor, ActorExecutor executor, SerializedObject serialized_init_args);
};
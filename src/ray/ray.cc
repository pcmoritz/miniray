#include "ray.h"

#include <iterator>

void Actor::Start(void* actor, ActorExecutor executor) {
  absl::MutexLock lock(&mu_);
  actor_ = actor;
  thread_ = std::thread([this, executor] {
    bool shutdown = false;
    while (!shutdown) {
      shutdown = ExecuteTasks(executor);
    }
  });
}

std::shared_ptr<Future> Actor::Submit(std::string& method_name, SerializedObject serialized_args) {
  auto result = std::make_shared<Future>();
  {
    absl::MutexLock lock(&mu_);
    queue_.emplace_back(Task({method_name, std::move(serialized_args), result}));
  }
  return result;
}

// Return true if the actor thread should be shut down
bool Actor::ExecuteTasks(const ActorExecutor& executor) {
  // Wait until new task is available and get the task
  std::deque<Task> tasks;
  {
    auto task_available = [this]() {
      mu_.AssertReaderHeld();
      return !queue_.empty();
    };
    absl::MutexLock lock(&mu_, absl::Condition(&task_available));
    std::swap(tasks, queue_);
  }
  // Now execute the tasks without holding the lock
  for (auto& task : tasks) {
    // If the actor creation task failed, propagate the error
    SerializedObject result =
      init_result_.error ? init_result_ :
      executor(actor_, task.method_name, task.serialized_args);
    task.result->set_ready(result);
    if (task.method_name == "__init__") {
      init_result_ = result;
    } else if (task.method_name == "__shutdown__") {
      return true;
    }
  }
  return false;
}

Actor::~Actor() {
  // Shut down the actor by sending a message to shut down and joining the thread.
  std::string shutdown_method_name = "__shutdown__";
  Submit(shutdown_method_name, SerializedObject());
  thread_.join();
}

Future::~Future() {
}

std::shared_ptr<Actor> Context::MakeActor(void* actor, ActorExecutor executor, SerializedObject serialized_init_args) {
  auto result = std::make_shared<Actor>();
  std::string init_method_name = "__init__";
  result->Submit(init_method_name, std::move(serialized_init_args));
  result->Start(actor, executor);
  return result;
}
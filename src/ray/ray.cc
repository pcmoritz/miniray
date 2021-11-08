#include "ray.h"

#include <iterator>

#include "util.h"

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

std::shared_ptr<Future> Actor::Submit(std::string& method_name, std::string& arg_data) {
  auto result = std::make_shared<Future>();
  Task task {std::move(method_name), std::move(arg_data), result};
  {
    FastMutexLock lock(&mu_);
    queue_.emplace_back(std::move(task));
  }
  return result;
}

// Return true if the actor thread should be shut down
bool Actor::ExecuteTasks(const ActorExecutor& executor) {
  // Wait until new task is available and get the task
  std::deque<Task> tasks;
  {
    FastMutexLock lock(&mu_);
    std::swap(tasks, queue_);
  }
  // Now execute the tasks without holding the lock
  for (auto& task : tasks) {
    if (task.method_name == "__shutdown__") {
      return true;
    }
    bool error_happened = false;
    std::string result = executor(actor_, task.method_name, task.args_data, &error_happened);
    task.result->set_ready(std::move(result));
  }
  return false;
}

Actor::~Actor() {
  // Shut down the actor by sending a message to shut down and joining the thread.
  std::string shutdown_method_name = "__shutdown__";
  std::string shutdown_args_data;
  Submit(shutdown_method_name, shutdown_args_data);
  thread_.join();
}

std::shared_ptr<Actor> Context::MakeActor(void* actor, ActorExecutor executor, std::string init_args_data) {
  auto result = std::make_shared<Actor>();
  std::string init_method_name = "__init__";
  result->Submit(init_method_name, init_args_data);
  result->Start(actor, executor);
  return result;
}

std::string Context::Get(const std::shared_ptr<Future>& future) {
  return future->data();
}
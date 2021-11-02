#include "ray.h"

void Context::ExecuteNextTask(ActorID actor_id, void** actor, ActorExecutor executor) {
  // Wait until new task is available and get the task
  TaskSpec task_spec;
  {
    auto task_available = [this, actor_id]() {
        mu_.AssertReaderHeld();
        return !actor_queues_[actor_id].queue.empty();
    };
    absl::MutexLock lock(&mu_, absl::Condition(&task_available));
    task_spec = std::move(actor_queues_[actor_id].queue.front());
    actor_queues_[actor_id].queue.pop_front();
  }
  // Now execute the task without holding the lock
  bool error_happened = false;
  std::string result = executor(actor, task_spec.method_name, task_spec.args_data, &error_happened);
}

ActorID Context::StartActor(ActorExecutor executor, const std::string& init_args_data) {
  absl::MutexLock lock(&mu_);
  int64_t actor_id = next_actor_id_;
  actors_[actor_id] = std::thread([this, actor_id, executor] {
    // Storage that the language implementation can use to store
    // state correspoinding to the actor (e.g. the PyObject* of
    // the object backing the actor).
    void* actor;
    while(true) {
      ExecuteNextTask(actor_id, &actor, executor);
    }
  });
  actor_queues_[actor_id].queue.emplace_back(TaskSpec{"__init__", init_args_data});
  next_actor_id_ += 1;
  return actor_id;
}

void Context::Submit(ActorID actor_id, std::string& method_name, std::string& arg_data) {
  TaskSpec task_spec {std::move(method_name), std::move(arg_data)};
  absl::MutexLock lock(&mu_);
  actor_queues_[actor_id].queue.emplace_back(std::move(task_spec));
}
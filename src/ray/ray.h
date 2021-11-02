#include <deque>
#include <string>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"

using ActorID = int64_t;

struct TaskSpec {
  std::string method_name;
  // Pickled collection of (args, kwargs)
  std::string args_data;
};

struct ActorQueue {
  std::deque<TaskSpec> queue;
};

using ActorExecutor = std::function<std::string(void**, const std::string&, const std::string&, bool* error_happened)>;

class Context {
public:
  Context() : next_actor_id_(0) {};
  ActorID StartActor(ActorExecutor executor, const std::string& init_args_data);
  void ExecuteNextTask(ActorID actor_id, void** actor, ActorExecutor executor);
  void Submit(ActorID actor_id, std::string& method_name, std::string& arg_data);
private:
  mutable absl::Mutex mu_;
  ActorID next_actor_id_ GUARDED_BY(mu_);
  absl::flat_hash_map<ActorID, std::thread> actors_ GUARDED_BY(mu_);
  absl::flat_hash_map<ActorID, ActorQueue> actor_queues_ GUARDED_BY(mu_);
};
#pragma once

#include "absl/synchronization/mutex.h"

class ABSL_SCOPED_LOCKABLE FastMutexLock {
public:
  explicit FastMutexLock(absl::Mutex* mu) ABSL_EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
    int64_t before = absl::GetCurrentTimeNanos();
    while (!mu_->TryLock()) {
      if (absl::GetCurrentTimeNanos() - before > 1000) {
        mu_->Lock();
        break;
      }
    }
  }

  FastMutexLock(const FastMutexLock&) = delete;
  FastMutexLock(FastMutexLock&&) = delete;
  FastMutexLock& operator=(const FastMutexLock&) = delete;
  FastMutexLock& operator=(FastMutexLock&&) = delete;

  ~FastMutexLock() ABSL_UNLOCK_FUNCTION() { mu_->Unlock(); }

private:
  absl::Mutex* const mu_;
};
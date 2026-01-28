#include <dripcore/scheduler.h>

namespace dripcore
{
  scheduler::scheduler(size_t num_executors)
    : executor_(std::make_unique<executor>())
  {
    // Phase 2: Always use single executor, ignore num_executors parameter
    // Phase 3+: Create num_executors executor instances
  }
  
  scheduler::~scheduler()
  {
    // executor_ automatically cleaned up by unique_ptr
  }
  
  void scheduler::run()
  {
    executor_->run();
  }
}

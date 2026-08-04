#ifndef DRIPCORE_CANCELLED_H
#define DRIPCORE_CANCELLED_H

#include <exception>

namespace dripcore
{
  // Exception thrown when a task is cancelled.
  // See doc/shutdown_and_cancellation_design.md for the full cancellation protocol.
  class cancelled : public std::exception
  {
  public:
    const char* what() const noexcept override
    {
      return "task cancelled";
    }
  };
}

#endif // DRIPCORE_CANCELLED_H
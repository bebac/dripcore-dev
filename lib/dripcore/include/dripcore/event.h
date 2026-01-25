#ifndef DRIPCORE_EVENT_H
#define DRIPCORE_EVENT_H

#include <unistd.h>
#include <fcntl.h>
#include <system_error>
#include <cstdint>

#ifdef __linux__
#include <sys/eventfd.h>
#endif

namespace dripcore
{
  class event
  {
  public:
    event()
    {
#ifdef __linux__
      if ( (fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) == -1 ) {
        throw std::system_error(errno, std::system_category());
      }
      write_fd_ = fd_;
#else
      // MacOS/BSD implementation using a self-pipe
      int pipe_fds[2];
      if ( pipe(pipe_fds) == -1 ) {
        throw std::system_error(errno, std::system_category());
      }
      fd_ = pipe_fds[0];       // Read end
      write_fd_ = pipe_fds[1]; // Write end

      // Set non-blocking on the read end so the scheduler won't block
      int flags = fcntl(fd_, F_GETFL, 0);
      if ( flags == -1 || fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == -1 ) {
        throw std::system_error(errno, std::system_category());
      }

      // Set cloexec for safety
      fcntl(fd_, F_SETFD, FD_CLOEXEC);
      fcntl(write_fd_, F_SETFD, FD_CLOEXEC);
#endif
    }

    ~event()
    {
      if ( fd_ != -1 ) {
        close(fd_);
      }
#ifndef __linux__
      if ( write_fd_ != -1 ) {
        close(write_fd_);
      }
#endif
    }

    // Signals the event (wakes up the scheduler)
    void notify()
    {
      uint64_t v = 1;
#ifdef __linux__
      if ( eventfd_write(fd_, v) < 0 ) {
        throw std::system_error(errno, std::system_category());
      }
#else
      if ( write(write_fd_, &v, sizeof(v)) < 0 ) {
        throw std::system_error(errno, std::system_category());
      }
#endif
    }

    // Clears the event status
    uint64_t read()
    {
      uint64_t v = 0;
#ifdef __linux__
      if ( eventfd_read(fd_, &v) < 0 ) {
        if ( errno == EAGAIN ) return 0;
        throw std::system_error(errno, std::system_category());
      }
#else
      if ( ::read(fd_, &v, sizeof(v)) < 0 ) {
        if ( errno == EAGAIN ) return 0;
        throw std::system_error(errno, std::system_category());
      }
#endif
      return v;
    }

    int native_handle() const
    {
      return fd_;
    }

  private:
    int fd_;
    int write_fd_;
  };
}

#endif // DRIPCORE_EVENT_H

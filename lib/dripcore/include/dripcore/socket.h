#ifndef DRIPCORE_SOCKET_H
#define DRIPCORE_SOCKET_H

// ----------------------------------------------------------------------------
#include <dripcore/socket_address.h>
#include <dripcore/socket_error.h>

// ----------------------------------------------------------------------------
#include <unistd.h>
#include <sys/ioctl.h>

// ----------------------------------------------------------------------------
#include <memory>
#include <system_error>

// ----------------------------------------------------------------------------
namespace dripcore
{
  class socket
  {
  public:
    explicit socket(int fd);
    socket(int domain, int type, int protocol);
    socket(socket&& other);
    socket(const socket& other);
  public:
    ~socket();
  public:
    socket& operator= (const socket& rhs);
  public:
    operator bool() const;
  public:
    void connect(const struct sockaddr *addr, socklen_t addrlen);
    void bind(const struct sockaddr *addr, socklen_t addrlen);
    void bind(const socket_address& addr);
    void listen(int backlog);
  public:
    socket accept(struct sockaddr *cliaddr, socklen_t *addrlen);
#if 0 // Drop support for Linux accept4.
    #ifdef __linux
    socket accept4(struct sockaddr *cliaddr, socklen_t *addrlen, int flags);
#endif
    socket accept(int flags);
    socket accept(socket_address& addr, int flags);
#endif
  public:
    ssize_t recv(void *buf, size_t len, int flags=0);
    ssize_t send(const void *buf, size_t len, int flags=0);
  public:
    void nonblocking(bool value = true);
    void reuseaddr(bool value = true);
    void keepalive(bool value = true);
  public:
    int platform_handle() const;
  private:
    class implementation;
    std::unique_ptr<implementation> pimpl_;
  };

  namespace ipv4
  {
    namespace tcp
    {
      inline dripcore::socket socket()
      {
        return dripcore::socket{AF_INET, SOCK_STREAM, IPPROTO_TCP};
      }
    }
  }
}

#endif // DRIPCORE_SOCKET_H

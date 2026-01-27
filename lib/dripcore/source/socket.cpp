#include <dripcore/socket.h>

// ----------------------------------------------------------------------------
#include <cassert>
#include <iostream>

#include <fcntl.h>

// ----------------------------------------------------------------------------
namespace dripcore
{
  class socket::implementation
  {
  public:
    explicit implementation(int fd) : fd_(fd)
    {
      //std::cout << "construct socket fd=" << fd << std::endl;
    }
  public:
    implementation(int domain, int type, int protocol)
    {
      if ( (fd_ = ::socket(domain, type, protocol)) < 0 ) {
        throw socket_error(errno);
      }
      //std::cout << "construct socket fd_=" << fd_ << std::endl;
    }
  public:
    ~implementation()
    {
      if ( !invalid() )
      {
        close(fd_);
        fd_ = -1;
      }
    }
  public:
    bool invalid() const
    {
      return fd_ == -1;
    }
  public:
    void connect(const struct sockaddr *addr, socklen_t addrlen)
    {
      if ( ::connect(fd_, (struct sockaddr *)addr, addrlen) < 0 ) {
        if ( errno != EINPROGRESS ) {
          throw socket_error(errno);
        }
      }
    }
  public:
    void bind(const struct sockaddr *addr, socklen_t addrlen)
    {
      if( ::bind(fd_, (struct sockaddr *)addr, addrlen) < 0 ) {
        throw socket_error(errno);
      }
    }
  public:
    void listen(int backlog)
    {
      if( ::listen(fd_, backlog) < 0 ) {
        throw socket_error(errno);
      }
    }
  public:
    socket accept(struct sockaddr *cliaddr, socklen_t *addrlen)
    {
      socket cli(::accept(fd_, cliaddr, addrlen));
      return cli;
    }
#ifdef __linux
  public:
    socket accept4(struct sockaddr *cliaddr, socklen_t *addrlen, int flags)
    {
      socket cli(::accept4(fd_, cliaddr, addrlen, flags));
      return cli;
    }
#endif
  public:
    ssize_t recv(void *buf, size_t len, int flags)
    {
      return ::recv(fd_, buf, len, flags|MSG_NOSIGNAL);
    }
  public:
    ssize_t send(const void *buf, size_t len, int flags)
    {
      return ::send(fd_, buf, len, flags|MSG_NOSIGNAL);
    }
  public:
    void nonblocking(bool value)
    {
      int blocking = ( value ? 1 : 0 );
      if ( ::ioctl(fd_, FIONBIO, (char*)&blocking) < 0 ) {
        throw socket_error(errno);
      }
    }
  public:
    void reuseaddr(bool value)
    {
      int v = ( value ? 1 : 0 );
      if ( setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(int)) == -1 ) {
        throw socket_error(errno);
      }
    }
  public:
    void keepalive(bool value = true)
    {
      int v = ( value ? 1 : 0 );
      if ( setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &v, sizeof(int)) == -1 ) {
        throw socket_error(errno);
      }
    }
  public:
    int platform_handle() const
    {
      return fd_;
    }
  private:
    int fd_;
  };

  socket::socket(int fd)
    : pimpl_(std::make_unique<implementation>(fd))
  {
  }

  socket::socket(int domain, int type, int protocol)
    : pimpl_(std::make_unique<implementation>(domain, type, protocol))
  {
  }

  socket::socket(socket&& other)
    : pimpl_(std::move(other.pimpl_))
  {
  }

  socket::socket(const socket& other)
  {
    *this = other;
  }

  socket::~socket()
  {
    //std::cout << "destruct socket fd_=" << platform_handle() << std::endl;
  }

  socket& socket::operator= (const socket& rhs)
  {
    pimpl_ = std::make_unique<implementation>(dup(rhs.platform_handle()));
    return *this;
  }

  socket::operator bool() const
  {
    assert(pimpl_);
    return !pimpl_->invalid();
  }

  void socket::connect(const struct sockaddr *addr, socklen_t addrlen)
  {
    assert(pimpl_);
    pimpl_->connect(addr, addrlen);
  }

  void socket::bind(const struct sockaddr *addr, socklen_t addrlen)
  {
    assert(pimpl_);
    pimpl_->bind(addr, addrlen);
  }

  void socket::bind(const socket_address& addr)
  {
    assert(pimpl_);
    pimpl_->bind(addr.ptr(), addr.len());
  }

  void socket::listen(int backlog)
  {
    assert(pimpl_);
    pimpl_->listen(backlog);
  }

  socket socket::accept(struct sockaddr *cliaddr, socklen_t *addrlen)
  {
    assert(pimpl_);
    return pimpl_->accept(cliaddr, addrlen);
  }

#if 0 // Drop support for Linux accept4.
#ifdef __linux
  socket socket::accept4(struct sockaddr *cliaddr, socklen_t *addrlen, int flags)
  {
    assert(pimpl_);
    return pimpl_->accept4(cliaddr, addrlen, flags);
  }
#endif

  socket socket::accept(int flags)
  {
    assert(pimpl_);
#ifdef __linux
    return pimpl_->accept4(0, 0, flags);
#else
    return pimpl_->accept(0, 0);
#endif
  }

  socket socket::accept(socket_address& addr, int flags)
  {
    assert(pimpl_);

    auto len    = socklen_t{addr.len()};
#ifdef __linux
    auto client = pimpl_->accept4(addr.ptr(), &len, flags);
#else
    //#error "unsupported platform"
    auto client = pimpl_->accept(addr.ptr(), &len);
    // if ( client )
    // {
    //   auto fd = client.platform_handle();
    //   int flags = fcntl(fd, F_GETFL, 0);
    //   fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    // }
#endif

    if ( client ) {
      assert(len == addr.len());
    }

    return client;
  }
#endif

  ssize_t socket::recv(void *buf, size_t len, int flags)
  {
    assert(pimpl_);
    return pimpl_->recv(buf, len, flags);
  }

  ssize_t socket::send(const void *buf, size_t len, int flags)
  {
    assert(pimpl_);
    return pimpl_->send(buf, len, flags);
  }

  void socket::nonblocking(bool value)
  {
    assert(pimpl_);
    pimpl_->nonblocking(value);
  }

  void socket::reuseaddr(bool value)
  {
    assert(pimpl_);
    pimpl_->reuseaddr(value);
  }

  void socket::keepalive(bool value)
  {
    assert(pimpl_);
    pimpl_->keepalive(value);
  }

  int socket::platform_handle() const
  {
    assert(pimpl_);
    return pimpl_->platform_handle();
  }
}

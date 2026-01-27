#ifndef DRIPCORE_SOCKET_ADDRESS_H
#define DRIPCORE_SOCKET_ADDRESS_H

// ----------------------------------------------------------------------------
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <system_error>

// ----------------------------------------------------------------------------
#include <netinet/in.h>
#include <arpa/inet.h>

// ----------------------------------------------------------------------------
namespace dripcore
{
  class socket_address
  {
  public:
    socket_address()
    {
      std::memset(&addr_, 0, sizeof(addr_));
    }
    socket_address(const char* ip, in_port_t port, sa_family_t family = AF_INET)
    {
      std::memset(&addr_, 0, sizeof(addr_));

      addr_.sin_family = family;
      addr_.sin_port   = htons(port);

      if ( auto res = inet_pton(family, ip, &addr_.sin_addr); res == 0 ) {
        throw std::runtime_error("invalid ip address");
      }
      else if ( res < 0 ) {
        throw std::system_error(errno, std::system_category());
      }
    }
  public:
    struct sockaddr* ptr() const
    {
      return reinterpret_cast<struct sockaddr*>(&addr_);
    }
  public:
    constexpr socklen_t len() const
    {
      return sizeof(sockaddr);
    }
  public:
    void write(std::ostream& os) const
    {
      char dst[16];

      if ( auto res = inet_ntop(addr_.sin_family, &addr_.sin_addr, dst, sizeof(dst)); res ) {
        os << std::string(res) << ':' << ntohs(addr_.sin_port);
      }
      else {
        os << strerror(errno);
      }
    }
  private:
    mutable struct sockaddr_in addr_;
  };

  inline std::ostream& operator<<(std::ostream& os, const socket_address& addr)
  {
    addr.write(os);
    return os;
  }
}

#endif // DRIPCORE_SOCKET_ADDRESS_H

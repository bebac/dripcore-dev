#ifndef DRIPCORE_SOCKET_ERROR_H
#define DRIPCORE_SOCKET_ERROR_H

// ----------------------------------------------------------------------------
#include <stdexcept>
#include <system_error>

// ----------------------------------------------------------------------------
namespace dripcore
{
  class socket_error : public std::system_error
  {
  public:
    socket_error(int ec) : std::system_error(ec, std::system_category())
    {
    }
    virtual ~socket_error() noexcept
    {
    }
  };
}

// ----------------------------------------------------------------------------
#endif // DRIPCORE_SOCKET_ERROR_H

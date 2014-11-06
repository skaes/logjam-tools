#include <zmq.h>
#include <czmq.h>
#include <limits.h>
#include "logjam-util.h"

uint64_t htonll(uint64_t net_number)
{
  uint64_t result = 0;
  for (int i = 0; i < (int)sizeof(result); i++) {
    result <<= CHAR_BIT;
    result += (((unsigned char *)&net_number)[i] & UCHAR_MAX);
  }
  return result;
}

uint64_t ntohll(uint64_t native_number)
{
  uint64_t result = 0;
  for (int i = (int)sizeof(result) - 1; i >= 0; i--) {
    ((unsigned char *)&result)[i] = native_number & UCHAR_MAX;
    native_number >>= CHAR_BIT;
  }
  return result;
}

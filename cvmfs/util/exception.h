/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_EXCEPTION_H_
#define CVMFS_UTIL_EXCEPTION_H_

#include <stdexcept>
#include <string>

#include "logging.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

class ECvmfsException : std::runtime_error {
 public:
  explicit ECvmfsException(const std::string& what_arg)
      : std::runtime_error(what_arg) {}
};

#define PANIC(...) Panic("PANIC: __FILE__ : __LINE__", kLogCvmfs, __VA_ARGS__);

__attribute__((noreturn))
void Panic(const char *coordinates, const LogSource source, const int mask,
           const char *format, ...);

// For PANIC(NULL)
__attribute__((noreturn))
void Panic(const char *coordinates, const LogSource source, const char *nul);

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_EXCEPTION_H_

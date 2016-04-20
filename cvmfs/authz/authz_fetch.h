/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_AUTHZ_AUTHZ_FETCH_H_
#define CVMFS_AUTHZ_AUTHZ_FETCH_H_

#include <inttypes.h>
#include <pthread.h>

#include <string>

#include "authz/authz.h"
#include "json_document.h"
#include "gtest/gtest_prod.h"
#include "util/single_copy.h"

class AuthzFetcher {
 public:
  /**
   * Based on the current client context (pid, uid, gid) and the given
   * membership requirement, gather credentials.  Positive and negative replies
   * have a time to live.
   */
  virtual AuthzStatus FetchWithinClientCtx(const std::string &membership,
                                           AuthzToken *authz_token,
                                           unsigned *ttl) = 0;
};


/**
 * Defines the result on construction.  Used in libcvmfs and for testing.
 */
class AuthzStaticFetcher : public AuthzFetcher {
 public:
  AuthzStaticFetcher(AuthzStatus s, unsigned ttl) : status_(s), ttl_(ttl) { }
  virtual AuthzStatus FetchWithinClientCtx(const std::string &membership,
                                           AuthzToken *authz_token,
                                           unsigned *ttl)
  {
    *authz_token = AuthzToken();
    *ttl = ttl_;
    return status_;
  }

 private:
  AuthzStatus status_;
  unsigned ttl_;
};


/**
 * Types of messages that can be sent between cvmfs client and authz helper.
 */
enum AuthzExternalMsgIds {
  kAuthzMsgHandshake = 0,  ///< Cvmfs: "Hello, helper, are you there?"
  kAuthzMsgReady,          ///< Helper: "Yes, cvmfs, I'm here"
  kAuthzMsgVerify,         ///< Cvmfs: "Please verify, helper"
  kAuthzMsgPermit,         ///< Helper: "I verified, cvmfs, here's the result"
  kAuthzMsgQuit,           ///< Cvmfs: "Please shutdown, helper"
  kAuthzMsgInvalid         ///< First invalid message id
};


/**
 * A binary representation of JSON messages that can be received from an authz
 * helper.  Holds either a kAuthzMsgReady or a kAuthzMsgPermit message.
 */
struct AuthzExternalMsg {
  AuthzExternalMsgIds msgid;
  int protocol_revision;
  struct {
    AuthzStatus status;
    AuthzToken token;
    uint32_t ttl;
  } permit;
};


/**
 * Connects to an external process that fetches the tokens.  The external helper
 * is spawned on demand through execve.  It has to receive commands on stdin
 * and write replies to stdout.  It can expect the following environment
 * variables to be set: CVMFS_FQRN, CVMFS_PID.
 */
class AuthzExternalFetcher : public AuthzFetcher, SingleCopy {
  FRIEND_TEST(T_AuthzFetch, ExecHelper);
  FRIEND_TEST(T_AuthzFetch, ExecHelperSlow);
  FRIEND_TEST(T_AuthzFetch, ParseMsg);
  FRIEND_TEST(T_AuthzFetch, Handshake);

 public:
  /**
   * The "wire" protocol: 4 byte version, 4 byte length, JSON message.  Must
   * be the same for cvmfs and helper.
   */
  static const uint32_t kProtocolVersion;  // = 1;

  AuthzExternalFetcher(const std::string &fqrn, const std::string &progname);
  AuthzExternalFetcher(const std::string &fqrn, int fd_send, int fd_recv);
  ~AuthzExternalFetcher();

  virtual AuthzStatus FetchWithinClientCtx(const std::string &membership,
                                           AuthzToken *authz_token,
                                           unsigned *ttl);

 private:
  /**
   * After 5 seconds of unresponsiveness, helper prcesses may be killed.
   */
  static const unsigned kChildTimeout = 5;

  /**
   * For now we allow "no caching".
   */
  static const int kMinTtl;  // = 0

  /**
   * If permits come without TTL, use 2 minutes.
   */
  static const unsigned kDefaultTtl = 120;

  void InitLock();
  void ExecHelper();
  bool Handshake();

  bool Send(const std::string &msg);
  bool Recv(std::string *msg);
  void EnterFailState();

  bool ParseMsg(const std::string &json_msg,
                const AuthzExternalMsgIds expected_msgid,
                AuthzExternalMsg *binary_msg);
  bool ParseMsgId(JSON *json_authz, AuthzExternalMsg *binary_msg);
  bool ParseRevision(JSON *json_authz, AuthzExternalMsg *binary_msg);
  bool ParsePermit(JSON *json_authz, AuthzExternalMsg *binary_msg);

  /**
   * The fully qualified repository name, e.g. atlas.cern.ch
   */
  std::string fqrn_;

  /**
   * Full path of external helper.
   */
  std::string progname_;

  /**
   * Send requests to the external helper.
   */
  int fd_send_;

  /**
   * Receive authz status, ttl, and token from the external helper.
   */
  int fd_recv_;

  /**
   * If a helper was started, the pid must be collected to avoid a zombie.
   */
  pid_t pid_;

  /**
   * If the external helper behaves unexectely, enter fail state and stop
   * authenticating
   */
  bool fail_state_;

  /**
   * The send-receive cycle is atomic.
   */
  pthread_mutex_t lock_;
};

#endif  // CVMFS_AUTHZ_AUTHZ_FETCH_H_

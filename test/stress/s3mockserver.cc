/**
 * This file is part of the CernVM File System.
 */
#include <errno.h>
#include <netinet/in.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <set>
#include <string>

#include "duplex_curl.h"
#include "hash.h"
#include "logging.h"
#include "util/string.h"

#define CVMFS_S3_TEST_MOCKUP_SERVER_PORT 8082

using namespace std;  // NOLINT

/**
 * Get the field number "idx" (e.g. 2) from a string
 * separated by given delimiter (e.g. ' '). The field numbering
 * starts from zero and minus values refer from the end of the
 * strings.
 */
std::string GetField(std::string str, char delim, int idx) {
  std::vector<std::string> str_vct = SplitString(str, delim);
  unsigned int idxu = idx % str_vct.size();
  return str_vct.at(idxu);
}


/**
 * Get value of the param string from the body. The value of the param
 * string is separated by delim1 and delim2. It starts with delim1 and ends
 * with delim2.
 * @param body Text body from where to search
 * @param param String to be searched from the body
 * @param delim1 param value is separated by this char
 * @param delim2 param value ends with this char
 * @return -1 if not found, otherwise value
 */
int GetValue(std::string body, std::string param,
              char delim1 = ':', char delim2 = ' ') {
  for (unsigned int i = 0; body.size()-param.size() > i; i++) {
    unsigned int j = 0;
    for (j = 0; param.size() > j; j++) {
      if (body.at(i+j) != param.at(j))
        break;
    }
    if (j == param.size()) {
      std::string l = GetField(body.substr(i), delim1, 1);
      l.erase(l.begin(), std::find_if(l.begin(), l.end(),
              std::not1(std::ptr_fun<int, int>(std::isspace))));
      return atoi(GetField(l, delim2, 0).c_str());
    }
  }
  return -1;
}

int main() {
  set<string> existing_files;

  int listen_sockfd, accept_sockfd;
  socklen_t clilen;
  struct sockaddr_in serv_addr, cli_addr;
  int retval = 0;

  // Listen incoming connections
  listen_sockfd = socket(AF_INET, SOCK_STREAM, 0);
  assert(listen_sockfd >= 0);
  bzero(reinterpret_cast<char *>(&serv_addr), sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(CVMFS_S3_TEST_MOCKUP_SERVER_PORT);
  int on = 1;
  retval = setsockopt(listen_sockfd, SOL_SOCKET,
                      SO_REUSEADDR, &on, sizeof(on));
  assert(retval == 0);
  retval = bind(listen_sockfd,
                (struct sockaddr *) &serv_addr,
                sizeof(serv_addr));
  assert(retval == 0);
  listen(listen_sockfd, 5);
  clilen = sizeof(cli_addr);

  fd_set rfds;
  bool quit = false;
  while (!quit) {
    // Wait for traffic
    FD_ZERO(&rfds);
    FD_SET(listen_sockfd, &rfds);
    retval = select(listen_sockfd+1, &rfds, NULL, NULL, NULL);
    assert(retval > 0);

    accept_sockfd = accept(listen_sockfd,
                            (struct sockaddr *) &cli_addr,
                            &clilen);
    assert(accept_sockfd >= 0);

    // Get header
    std::string req_header = "";
    char buf[10000];
    int nread = read(accept_sockfd, buf, 10000);
    buf[nread] = 0;
    char *occ = strstr(buf, "\r\n\r\n");
    if (!occ) occ = strstr(buf, "\n\n");
    assert(occ);
    req_header += std::string(buf, occ-buf);

    // Parse header
    std::string req_type = "";
    std::string req_file = "";  // target name without bucket prefix
    int content_length = 0;
    req_type = GetField(req_header, ' ', 0);
    req_file = GetField(req_header, ' ', 1);
    req_file = req_file.substr(req_file.find("/", 1) + 1);  // no bucket
    if (req_type.compare("PUT") == 0) {
      content_length = GetValue(req_header, "Content-Length");
      assert(content_length >= 0);
    }

    string reply = "HTTP/1.1 200 OK\r\n";

    if (req_type.compare("PUT") == 0) {
      existing_files.insert(req_file);
    } else if (req_type.compare("HEAD") == 0) {
      if (existing_files.find(req_file) == existing_files.end()) {
        reply = "HTTP/1.1 404 Not Found\r\n";
      } else {
        reply = "HTTP/1.1 200 OK\r\n";
      }
    } else if (req_type.compare("DELETE") == 0) {
      existing_files.erase(req_file);
      // "No Content"-reply even if file did not exist
      reply = "HTTP/1.1 204 No Content\r\n";
    }
    reply += "Content-Length: 0\r\n";
    reply += "Connection: close\r\n\r\n";

    int n = write(accept_sockfd, reply.c_str(), reply.length());
    assert(n >= 0);
    close(accept_sockfd);
  }
  return 0;
}

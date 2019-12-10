/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SWISSKNIFE_CATALOG_STATS_H_
#define CVMFS_SWISSKNIFE_CATALOG_STATS_H_

#include <pthread.h>

#include "catalog_traversal.h"
#include "hash.h"
#include "smallhash.h"

#include "swissknife.h"

namespace swissknife {

struct CatalogHashSet {
  CatalogHashSet() {
    shash::Any empty_hash = shash::Any();
    empty_hash.SetNull();
    objects.Init(64, empty_hash, CatalogHashSet::hasher);
    subcats.Init(64, empty_hash, CatalogHashSet::hasher);
  }
  
  static uint32_t hasher(const shash::Any &key) {
    // Don't start with the first bytes, because == is using them as well
    return (uint32_t) *(reinterpret_cast<const uint32_t *>(key.digest) + 1);
  }

  int revision;
  SmallHashDynamic<shash::Any, bool> objects;
  SmallHashDynamic<shash::Any, bool> subcats;
};

struct ResultEntry {
  ResultEntry(std::string path, CatalogHashSet *old, CatalogHashSet *cur) : path(path) {
    prev_revision = old->revision;
    revision = cur->revision;
    
    prev_num_objects = old->objects.size();
    num_objects = cur->objects.size();
    num_common_objects = 0;
    for (uint32_t i = 0; i < old->objects.capacity(); ++i) {
      if (old->objects.keys()[i] != old->objects.empty_key()) {
        if (cur->objects.Contains(old->objects.keys()[i])) {
          ++num_common_objects;
        }
      }
    }

    prev_num_subcats = old->subcats.size();
    num_subcats = cur->subcats.size();
    num_common_subcats = 0;
    for (uint32_t i = 0; i < old->subcats.capacity(); ++i) {
      if (old->subcats.keys()[i] != old->subcats.empty_key()) {
        if (cur->subcats.Contains(old->subcats.keys()[i])) {
          ++num_common_subcats;
        }
      }
    }
  }
  
  ResultEntry(std::string path, CatalogHashSet *cur) : path(path) {
    prev_revision = 0;
    revision = cur->revision;
    
    prev_num_objects = 0;
    num_objects = cur->objects.size();
    num_common_objects = 0;
    
    prev_num_subcats = 0;
    num_subcats = cur->subcats.size();
    num_common_subcats = 0;
  }
  std::string path;
  uint64_t prev_revision;
  uint64_t revision;
  uint64_t prev_num_objects;
  uint64_t num_objects;
  uint64_t num_common_objects;
  uint64_t num_subcats;
  uint64_t prev_num_subcats;
  uint64_t num_common_subcats;
};

class CommandCatalogStats : public Command {
 public:
  ~CommandCatalogStats() {}
  virtual std::string GetName() const { return "catalog_stats"; }
  virtual std::string GetDescription() const {
    return "CernVM-FS catalog tree statistics export tool.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  template <class ObjectFetcherT>
  bool Run(ObjectFetcherT *object_fetcher, uint64_t rev_start = -1, uint64_t rev_end = -1);

  static uint32_t hasher_string(const std::string &str) {
    uint32_t hash = 0x811c9dc5;
    uint32_t prime = 0x1000193;
    for (std::string::const_iterator it = str.begin(); it != str.end(); ++it) {
      uint8_t val = *it;
      hash = hash ^ val;
      hash *= prime;
    }
    return hash;
  }

  template <class ObjectFetcherT>
  void ProcessCatalog(const typename CatalogTraversal<ObjectFetcherT>::CallbackDataTN &data);
  std::string PrintResults(uint64_t root_revision);

  std::string repo_name_;
  shash::Any reflog_hash_;
  std::string db_path_;

  pthread_mutex_t hash_sets_mutex_;
  SmallHashDynamic<std::string, CatalogHashSet *> hash_sets_;
  std::vector<ResultEntry> results_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_CATALOG_STATS_H_
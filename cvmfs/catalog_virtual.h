/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CATALOG_VIRTUAL_H_
#define CVMFS_CATALOG_VIRTUAL_H_

#include "swissknife_assistant.h"

#include <string>
#include <vector>

#include "hash.h"

namespace catalog {
class WritableCatalogManager;
}
namespace download {
class DownloadManager;
}
namespace manifest {
class Manifest;
}
struct SyncParameters;


namespace catalog {

class VirtualCatalog {
 public:
  static const std::string kVirtualPath;

  VirtualCatalog(manifest::Manifest *m,
                 download::DownloadManager *d,
                 catalog::WritableCatalogManager *c,
                 SyncParameters *p);
  void GenerateSnapshots();

 private:
  static const std::string kSnapshotDirectory;

  struct TagId {
    TagId() { }
    TagId(const std::string &n, const shash::Any &h) : name(n), hash(h) { }
    bool operator ==(const TagId &other) const {
      return (this->name == other.name) && (this->hash == other.hash);
    }
    bool operator <(const TagId &other) const {
      if (this->name < other.name) { return true; }
      else if (this->name > other.name) { return false; }
      return this->hash < other.hash;
    }

    std::string name;
    shash::Any hash;
  };

  void EnsurePresence();
  void CreateCatalog();
  void CreateBaseDirectory();
  void CreateNestedCatalogMarker();
  void CreateSnapshotDirectory();
  void GetSortedTagsFromHistory(std::vector<TagId> *tags);
  void GetSortedTagsFromCatalog(std::vector<TagId> *tags);
  void RemoveSnapshot(TagId tag);
  void InsertSnapshot(TagId tag);

  catalog::WritableCatalogManager *catalog_mgr_;
  swissknife::Assistant assistant_;
};  // class VirtualCatalog

}  // namespace catalog

#endif  // CVMFS_CATALOG_VIRTUAL_H_

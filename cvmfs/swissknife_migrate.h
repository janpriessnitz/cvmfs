/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_MIGRATE_H_
#define CVMFS_SWISSKNIFE_MIGRATE_H_

#include "swissknife.h"

#include "hash.h"
#include "util_concurrency.h"
#include "catalog.h"
#include "upload.h"

#include <map>

namespace catalog {
  class WritableCatalog;
}

namespace swissknife {

class CommandMigrate : public Command {
 protected:
  typedef
    std::vector<Future<catalog::Catalog::NestedCatalog>* >
    FutureNestedCatalogList;

  struct PendingCatalog {
    PendingCatalog(const bool                               success        = false,
                   catalog::WritableCatalog                *new_catalog    = NULL,
                   Future<catalog::Catalog::NestedCatalog> *new_nested_ref = NULL) :
      success(success),
      new_catalog(new_catalog),
      new_nested_ref(new_nested_ref) {}

    bool                                      success;
    catalog::WritableCatalog                 *new_catalog;
    Future<catalog::Catalog::NestedCatalog>  *new_nested_ref;
  };

  class PendingCatalogMap : public std::map<std::string, PendingCatalog>,
                            public Lockable {};

  class MigrationWorker : public ConcurrentWorker<MigrationWorker> {
   public:
    struct expected_data {
      expected_data(
        const catalog::Catalog                         *catalog,
              Future<catalog::Catalog::NestedCatalog>  *new_nested_ref,
        const FutureNestedCatalogList                  &future_nested_catalogs) :
        catalog(catalog),
        new_nested_ref(new_nested_ref),
        future_nested_catalogs(future_nested_catalogs) {}
      expected_data() :
        catalog(NULL),
        new_nested_ref(NULL) {}

      const catalog::Catalog                         *catalog;
            Future<catalog::Catalog::NestedCatalog>  *new_nested_ref;
      const FutureNestedCatalogList                   future_nested_catalogs;
    };

    typedef PendingCatalog returned_data;

    struct worker_context {
      worker_context(const std::string temporary_directory) :
        temporary_directory(temporary_directory) {}

      const std::string temporary_directory;
    };

   public:
    MigrationWorker(const worker_context *context);
    virtual ~MigrationWorker();

    void operator()(const expected_data &data);

   protected:
    catalog::WritableCatalog* CreateNewEmptyCatalog(
                                            const std::string &root_path) const;
    bool MigrateFileMetadata(const catalog::Catalog    *catalog,
                             catalog::WritableCatalog  *writable_catalog) const;

   private:
    const std::string temporary_directory_;
  };

 public:
  CommandMigrate();
  ~CommandMigrate() { };
  std::string GetName() { return "migrate"; };
  std::string GetDescription() {
    return "CernVM-FS catalog repository migration \n"
      "This command migrates the whole catalog structure of a given repository";
  };
  ParameterList GetParams();

  int Main(const ArgumentList &args);


 protected:
  void CatalogCallback(const catalog::Catalog* catalog,
                       const hash::Any&        catalog_hash,
                       const unsigned          tree_level);
  void MigrationCallback(const PendingCatalog &data);
  void UploadCallback(const upload::SpoolerResult &result);

  void ConvertCatalogsRecursively(
              const catalog::Catalog                         *catalog,
                    Future<catalog::Catalog::NestedCatalog>  *new_catalog);
  void ConvertCatalog(
        const catalog::Catalog                         *catalog,
              Future<catalog::Catalog::NestedCatalog>  *new_catalog,
        const FutureNestedCatalogList                  &future_nested_catalogs);

 private:
  bool              print_tree_;
  bool              print_hash_;

  catalog::Catalog const*                        root_catalog_;
  UniquePtr<ConcurrentWorkers<MigrationWorker> > concurrent_migration;
  UniquePtr<upload::Spooler>                     spooler_;
  PendingCatalogMap                              pending_catalogs_;
};

}

#endif  // CVMFS_SWISSKNIFE_MIGRATE_H_

/**
 * This file is part of the CernVM File System
 */


#include "object_fetcher.h"
#include "reflog.h"
#include "util/posix.h"
#include "util/string.h"

#include "swissknife_catalog_stats.h"

namespace swissknife {

ParameterList CommandCatalogStats::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory(
              'r', "repository URL (absolute local path or remote URL)"));
  r.push_back(Parameter::Mandatory(
              'o', "output database file "
              "(new or created by previous catalog_stats command)"));
  r.push_back(Parameter::Optional('n', "fully qualified repository name"));
  r.push_back(Parameter::Optional('k', "repository master key(s) / dir"));
  r.push_back(Parameter::Optional('R', "path to reflog.chksum file"));
  r.push_back(Parameter::Optional('t', "temporary directory"));
  r.push_back(Parameter::Optional(
              's', "starting revision (default: earliest possible)"));
  r.push_back(Parameter::Optional('e', "ending revision (default: current)"));
  return r;
}

int CommandCatalogStats::Main(const ArgumentList &args) {
  const std::string &repo_url  = *args.find('r')->second;
  db_path_ = *args.find('o')->second;
  const std::string &repo_name_ =
    (args.count('n') > 0) ? *args.find('n')->second : "";
  std::string repo_keys =
    (args.count('k') > 0) ? *args.find('k')->second : "";
  if (DirectoryExists(repo_keys))
    repo_keys = JoinStrings(FindFilesBySuffix(repo_keys, ".pub"), ":");
  const std::string &reflog_chksum_path = (args.count('R') > 0) ?
    *args.find('R')->second : "";
  const std::string &tmp_dir =
    (args.count('t') > 0) ? *args.find('t')->second : "/tmp";

  const uint64_t &rev_start =
    (args.count('s') > 0) ? String2Int64(*args.find('s')->second) : 0;
  const uint64_t &rev_end =
    (args.count('e') > 0) ? String2Int64(*args.find('e')->second) : 0;

  if (reflog_chksum_path != "") {
    if (!manifest::Reflog::ReadChecksum(reflog_chksum_path, &reflog_hash_)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Could not read reflog checksum");
      return 1;
    }
  }

  bool success;
  if (IsHttpUrl(repo_url)) {
    const bool follow_redirects = false;
    if (!this->InitDownloadManager(follow_redirects) ||
        !this->InitVerifyingSignatureManager(repo_keys)) {
      LogCvmfs(kLogCatalog, kLogStderr, "Failed to init remote connection");
      return 1;
    }

    HttpObjectFetcher<catalog::Catalog,
                      history::SqliteHistory> fetcher(repo_name_,
                                                      repo_url,
                                                      tmp_dir,
                                                      download_manager(),
                                                      signature_manager());
    if (reflog_hash_.IsNull()) {
      manifest::Manifest *manifest;
      ObjectFetcherFailures::Failures failure =
        fetcher.FetchManifest(&manifest);
      if (failure != ObjectFetcherFailures::kFailOk) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to fetch manifest: %s",
                    Code2Ascii(failure));
        return 1;
      }
      reflog_hash_ = manifest->reflog_hash();
    }

    success = Run(&fetcher, rev_start, rev_end);
  } else {
    LocalObjectFetcher<> fetcher(repo_url, tmp_dir);

    if (reflog_hash_.IsNull()) {
      manifest::Manifest *manifest;
      ObjectFetcherFailures::Failures failure =
        fetcher.FetchManifest(&manifest);
      if (failure != ObjectFetcherFailures::kFailOk) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Failed to fetch manifest: %s",
                    Code2Ascii(failure));
        return 1;
      }
      reflog_hash_ = manifest->reflog_hash();
    }

    success = Run(&fetcher, rev_start, rev_end);
  }

  return success ? 0 : 1;
}

template <class ObjectFetcherT>
bool CommandCatalogStats::Run(ObjectFetcherT *object_fetcher,
                              uint64_t rev_start, uint64_t rev_end) {
  pthread_mutex_init(&hash_sets_mutex_, NULL);
  hash_sets_.Init(128, "\\", hasher_string);

  typename CatalogTraversal<ObjectFetcherT>::Parameters params;
  params.object_fetcher = object_fetcher;
  params.no_repeat_history = true;
  CatalogTraversal<ObjectFetcherT> traversal(params);
  traversal.RegisterListener(&CommandCatalogStats::ProcessCatalog<ObjectFetcherT>, this);

  typename manifest::Reflog *reflog;
  reflog = FetchReflog(object_fetcher, repo_name_, reflog_hash_);

  std::vector<shash::Any> root_catalogs;
  reflog->List(SqlReflog::kRefCatalog, &root_catalogs); // listing ordered by date desc
  for (int i = root_catalogs.size() - 1; i >= 0; --i) {
    shash::Any root_catalog_hash = root_catalogs[i];
    typename ObjectFetcherT::CatalogTN *catalog;
    object_fetcher->FetchCatalog(root_catalog_hash, "", &catalog);
    // printf("revision: %lu\n", catalog->revision());
    uint64_t root_revision = catalog->revision();
    if (rev_start != 0 && rev_start > catalog->revision()) {
      continue;
    }
    if (rev_end != 0 && rev_end < catalog->revision()) {
      break;
    }
    delete catalog;
    traversal.TraverseRevision(root_catalog_hash);
    printf("%s", PrintResults(root_revision).c_str());
    results_.clear();
  }
  return true;
}

template <class ObjectFetcherT>
void CommandCatalogStats::ProcessCatalog(const typename CatalogTraversal<ObjectFetcherT>::CallbackDataTN &data) {
  CatalogHashSet *cur_hash_set = new CatalogHashSet;
  std::string cur_mountpoint = data.catalog->mountpoint().ToString();
  cur_hash_set->revision = data.catalog->revision();

  catalog::Catalog::HashVector objects;
  assert(data.catalog->HashListing(&objects));
  catalog::Catalog::HashVector::const_iterator it = objects.begin();
  for (; it != objects.end(); ++it) {
    cur_hash_set->objects.Insert(*it, true);
  }

  const catalog::Catalog::NestedCatalogList subcats = data.catalog->ListNestedCatalogs();
  catalog::Catalog::NestedCatalogList::const_iterator it_subcats = subcats.begin();
  for(; it_subcats != subcats.end(); ++it_subcats) {
    cur_hash_set->subcats.Insert((*it_subcats).hash, true);  // TODO what if mountpoint changes??
  }

  MutexLockGuard guard(hash_sets_mutex_);
  CatalogHashSet *prev_hash_set;
  if (hash_sets_.Lookup(cur_mountpoint, &prev_hash_set)) {
    results_.push_back(ResultEntry(cur_mountpoint, prev_hash_set, cur_hash_set));
    delete prev_hash_set;
  } else {
    results_.push_back(ResultEntry(cur_mountpoint, cur_hash_set));
  }
  hash_sets_.Insert(cur_mountpoint, cur_hash_set);
}

std::string CommandCatalogStats::PrintResults(uint64_t root_revision) {
  std::string result;
  result = "root_rev,old_rev,new_rev,path,old_num_obj,new_num_obj,common_num_obj,old_num_cats,new_num_cats,common_num_cats\n";
  for(std::vector<ResultEntry>::const_iterator it = results_.begin(); it != results_.end(); ++it) {
    result += StringifyUint(root_revision) + "," + StringifyUint(it->prev_revision) + "," + StringifyUint(it->revision) + "," +
              it->path + "," + StringifyUint(it->prev_num_objects) + "," + StringifyUint(it->num_objects) + "," + StringifyUint(it->num_common_objects) + "," +
              StringifyUint(it->prev_num_subcats) + "," + StringifyUint(it->num_subcats) + "," + StringifyUint(it->num_common_subcats) + "\n"; 
  }
  return result;
}


}  // namespace swissknife
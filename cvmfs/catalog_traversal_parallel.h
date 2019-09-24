/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_
#define CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_

#include <vector>

#include "catalog_traversal.h"


namespace swissknife {

template<class ObjectFetcherT>
class CatalogTraversalParallel : public CatalogTraversal<ObjectFetcherT> {
 public:
  typedef ObjectFetcherT                      ObjectFetcherTN;
  typedef typename ObjectFetcherT::CatalogTN  CatalogTN;
  typedef typename ObjectFetcherT::HistoryTN  HistoryTN;
  typedef CatalogTraversalData<CatalogTN>     CallbackDataTN;
  typedef typename CatalogTN::NestedCatalogList NestedCatalogList;

  typedef typename CatalogTraversal<ObjectFetcherT>::Parameters Parameters;
  // typedef typename CatalogTraversal<ObjectFetcherT>::TraversalType TraversalType;

  explicit CatalogTraversalParallel(const Parameters &params) : CatalogTraversal<ObjectFetcherT>(params) {
    finished_ = false;
    atomic_init32(&root_catalogs_unprocessed_);
    shash::Any null_hash;
    null_hash.SetNull();
    catalogs_processing_.Init(1024, null_hash, hasher);
    catalogs_done_.Init(1024, null_hash, hasher);
    pthread_mutex_init(&catalogs_hash_lock_, NULL);
    pthread_mutex_init(&job_queue_lock_, NULL);
  }

  enum TraversalType {
    kBreadthFirstTraversal,
    kDepthFirstTraversal
  };

 protected:
  struct CatalogJob : public CatalogTraversal<ObjectFetcherT>::CatalogJob, public Observable<int>  {
    explicit CatalogJob(const std::string  &path,
                        const shash::Any   &hash,
                        const unsigned      tree_level,
                        const unsigned      history_depth,
                        CatalogTN    *parent = NULL) : CatalogTraversal<ObjectFetcherT>::CatalogJob(path, hash, tree_level, history_depth, parent) {
      atomic_init32(&children_unprocessed);
      state = kStatePre;
    }

    void WakeParents() {
      this->NotifyListeners(0);
    }

    enum JobState {
      kStatePre,
      kStatePost
    };

    JobState state;
    atomic_int32 children_unprocessed;
  };

 public:
  /**
   * Starts the traversal process.
   * After calling this methods CatalogTraversal will go through all catalogs
   * and call the registered callback methods for each found catalog.
   * If something goes wrong in the process, the traversal will be cancelled.
   *
   * @param type   breadths or depth first traversal
   * @return       true, when all catalogs were successfully processed. On
   *               failure the traversal is cancelled and false is returned.
   */
  bool Traverse(const TraversalType type = kBreadthFirstTraversal) {
    const shash::Any root_catalog_hash = this->GetRepositoryRootCatalogHash();
    if (root_catalog_hash.IsNull()) {
      return false;
    }
    return Traverse(root_catalog_hash, type);
  }

  /**
   * Starts the traversal process at the catalog pointed to by the given hash
   *
   * @param root_catalog_hash  the entry point into the catalog traversal
   * @param type               breadths or depth first traversal
   * @return                   true when catalogs were successfully traversed
   */
  bool Traverse(const shash::Any &root_catalog_hash, const TraversalType type = kBreadthFirstTraversal) {
    // add the root catalog of the repository as the first element on the job
    // stack

    CatalogJob *root_job = new CatalogJob("", root_catalog_hash, 0, 0);
    PushJobToQueue(root_job);
    atomic_inc32(&root_catalogs_unprocessed_);
    return DoTraverse();
  }

  bool TraverseList(const std::vector<shash::Any> &root_catalog_list) {
    std::vector<shash::Any>::const_iterator i    = root_catalog_list.begin();
    const std::vector<shash::Any>::const_iterator iend = root_catalog_list.end();
    for(; i != iend; ++i) {
      CatalogJob *root_job = new CatalogJob("", *i, 0, 0);
      PushJobToQueue(root_job);
      atomic_inc32(&root_catalogs_unprocessed_);
    }
    return DoTraverse();
  }

  /**
   * Starts the traversal process at the catalog pointed to by the given hash
   * but doesn't traverse into predecessor catalog revisions. This overrides the
   * TraversalParameter settings provided at construction.
   *
   * @param root_catalog_hash  the entry point into the catalog traversal
   * @param type               breadths or depth first traversal
   * @return                   true when catalogs were successfully traversed
   */
  bool TraverseRevision(const shash::Any     &root_catalog_hash,
                        const TraversalType  type = kBreadthFirstTraversal) {

    return Traverse(root_catalog_hash, type);
  }

  /**
   * Figures out all named tags in a repository and uses all of them as entry
   * points into the traversal process.
   *
   * @param type  breadths or depth first traversal
   * @return      true when catalog traversal successfully finished
   */
  bool TraverseNamedSnapshots(const TraversalType type = kBreadthFirstTraversal)
  {
    printf("Named snapshots begin\n");
    typedef std::vector<shash::Any> HashList;

    UniquePtr<HistoryTN> tag_db;
    const typename ObjectFetcherT::Failures retval =
                                         this->object_fetcher_->FetchHistory(&tag_db);
    switch (retval) {
      case ObjectFetcherT::kFailOk:
        break;

      case ObjectFetcherT::kFailNotFound:
        LogCvmfs(kLogCatalogTraversal, kLogDebug,
                 "didn't find a history database to traverse");
        return true;

      default:
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
                 "failed to download history database (%d - %s)",
                 retval, Code2Ascii(retval));
        return false;
    }

    HashList root_hashes;
    bool success = tag_db->GetHashes(&root_hashes);
    assert(success);

          HashList::const_iterator i    = root_hashes.begin();
    const HashList::const_iterator iend = root_hashes.end();
    for (; i != iend; ++i) {
      CatalogJob *new_job = new CatalogJob("", *i, 0, 0);
      PushJobToQueue(new_job);
      atomic_inc32(&root_catalogs_unprocessed_);
    }
    printf("Named snapshots: DoTraverse\n");

    return DoTraverse();
  }

 protected:

  static uint32_t hasher(const shash::Any &key) {
    // Don't start with the first bytes, because == is using them as well
    return (uint32_t) *(reinterpret_cast<const uint32_t *>(key.digest) + 1);
  }

  bool DoTraverse() {
    int num_threads = GetNumberOfCpuCores();
    threads_process_ = reinterpret_cast<pthread_t *> (smalloc(sizeof(pthread_t)*num_threads));
    finished_ = false;
    for (int i = 0; i < num_threads; ++i) {
      int retval = pthread_create(&threads_process_[i], NULL, ProcessQueue, this);
      assert(retval == 0);
    }
    while(atomic_read32(&root_catalogs_unprocessed_));
    finished_ = true;
    for (int i = 0; i < num_threads; ++i) {
      pthread_join(threads_process_[i], NULL);
    }
    free(threads_process_);

    assert(catalogs_processing_.size() == 0);
    assert(pre_job_queue_.empty());
    assert(post_job_queue_.empty());
    return true;
  }

  static void *ProcessQueue(void *data) {
    CatalogTraversalParallel<ObjectFetcherT> *traversal = reinterpret_cast<CatalogTraversalParallel<ObjectFetcherT> *>(data);
    CatalogJob *current_job;
    while(!traversal->finished_) {
      if (!(current_job = traversal->GrabJobFromQueue())) continue;
      if (current_job->state == CatalogJob::kStatePre) {
        traversal->ProcessJobPre(current_job);
      } else {
        traversal->ProcessJobPost(current_job);
      }
    }
    return NULL;
  }

  CatalogJob *GrabJobFromQueue() {
    MutexLockGuard m(&job_queue_lock_);
    if (post_job_queue_.empty()) {
      if (pre_job_queue_.empty()) {
        return NULL;
      }
      CatalogJob *result = pre_job_queue_.front();
      pre_job_queue_.pop();
      return result;
    } else {
      CatalogJob *result = post_job_queue_.front();
      post_job_queue_.pop();
      return result;
    }
  }

  void PushJobToQueue(CatalogJob *job) {
    MutexLockGuard m(&job_queue_lock_);
    if (job->state == CatalogJob::kStatePre) {
      pre_job_queue_.push(job);
    } else {
      post_job_queue_.push(job);
    }
  }

  void ProcessJobPre(CatalogJob *job) {
    bool success = this->PrepareCatalog(job);
    if (job->ignore || !success) {
      return;
    }

    NestedCatalogList catalog_list = job->catalog->ListOwnNestedCatalogs();
    this->CloseCatalog(false, job);
    job->state = CatalogJob::kStatePost;

    unsigned int num_children = PushNestedCatalogs(job, catalog_list);
    // No children will wake up and push the job -> need to do it here
    if (num_children == 0) {
      PushJobToQueue(job);
    }
  }

  unsigned int PushNestedCatalogs(CatalogJob *job, const NestedCatalogList &catalog_list) {
    MutexLockGuard m(&catalogs_hash_lock_);
    typename NestedCatalogList::const_iterator i = catalog_list.begin();
    typename NestedCatalogList::const_iterator iend = catalog_list.end();
    unsigned int num_children = 0;
    for(; i != iend; ++i) {
      if (catalogs_done_.Contains(i->hash)) {
        continue;
      }

      CatalogJob *child;
      if (!catalogs_processing_.Lookup(i->hash, &child)) {
        child = new CatalogJob(i->mountpoint.ToString(),
                               i->hash,
                               job->tree_level + 1,
                               job->history_depth);

        PushJobToQueue(child);
        catalogs_processing_.Insert(i->hash, child);
      }

      // child->RegisterListener(MakeClosure(&CatalogTraversalParallel::OnChildFinished, this, job));
      // BoundClosure<void, CatalogTraversalParallel<ObjectFetcherT>, CatalogJob *> closure()
      child->RegisterListener(&CatalogTraversalParallel::OnChildFinished, this, job);
      ++num_children;
    }

    atomic_write32(&job->children_unprocessed, num_children);
    return num_children;
  }

  void ProcessJobPost(CatalogJob *job) {
    this->ReopenCatalog(job);
    this->NotifyListeners(job->GetCallbackData());

    {
      MutexLockGuard m(&catalogs_hash_lock_);
      catalogs_processing_.Erase(job->hash);
      catalogs_done_.Insert(job->hash, true);
    }

    if(job->IsRootCatalog()) {
      atomic_dec32(&root_catalogs_unprocessed_);
    } else {
      job->WakeParents();
    }
    this->CloseCatalog(true, job);
    delete job;
  }

  void OnChildFinished(const int &a, CatalogJob *job) {
    // atomic_xadd32 returns value before subtraction -> needs to equal 1
    if (atomic_xadd32(&job->children_unprocessed, -1) == 1) {
      PushJobToQueue(job);
    }
  }

  std::queue<CatalogJob *> pre_job_queue_;
  std::queue<CatalogJob *> post_job_queue_;
  pthread_mutex_t job_queue_lock_;

  pthread_t *threads_process_;
  volatile bool finished_;
  atomic_int32 root_catalogs_unprocessed_;

  SmallHashDynamic<shash::Any, CatalogJob *> catalogs_processing_;
  SmallHashDynamic<shash::Any, bool> catalogs_done_;
  pthread_mutex_t catalogs_hash_lock_;
};

}  // namespace swissknife

#endif  // CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_

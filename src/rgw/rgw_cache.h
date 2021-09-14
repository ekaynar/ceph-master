// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGWCACHE_H
#define CEPH_RGWCACHE_H

#include <string>
#include <map>
#include <unordered_map>
#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"

#include "cls/version/cls_version_types.h"
#include "rgw_common.h"


/*datacache*/
#include <errno.h>
#include <unistd.h> 
#include <signal.h> 
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "include/Context.h"
#include <aio.h>
#include "rgw_threadpool.h"
#include "rgw_cacherequest.h"
#include <curl/curl.h>
#include "include/lru.h"
// #include <mutex> 
// #include "common/RWLock.h"
struct cache_obj;
class CopyRemoteS3Object;
class CacheThreadPool;
struct RemoteRequest;
struct ObjectDataInfo;
class RemoteS3Request;
class RGWGetDataCB;
struct DataCache;
/*datacache*/


enum {
  UPDATE_OBJ,
  REMOVE_OBJ,
};

#define CACHE_FLAG_DATA           0x01
#define CACHE_FLAG_XATTRS         0x02
#define CACHE_FLAG_META           0x04
#define CACHE_FLAG_MODIFY_XATTRS  0x08
#define CACHE_FLAG_OBJV           0x10

struct ObjectMetaInfo {
  uint64_t size;
  real_time mtime;

  ObjectMetaInfo() : size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 2, bl);
    encode(size, bl);
    encode(mtime, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, bl);
    decode(size, bl);
    decode(mtime, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectMetaInfo*>& o);
};
WRITE_CLASS_ENCODER(ObjectMetaInfo)

  struct ObjectCacheInfo {
    int status = 0;
    uint32_t flags = 0;
    uint64_t epoch = 0;
    bufferlist data;
    map<string, bufferlist> xattrs;
    map<string, bufferlist> rm_xattrs;
    ObjectMetaInfo meta;
    obj_version version = {};
    ceph::coarse_mono_time time_added;

    ObjectCacheInfo() = default;

    void encode(bufferlist& bl) const {
      ENCODE_START(5, 3, bl);
      encode(status, bl);
      encode(flags, bl);
      encode(data, bl);
      encode(xattrs, bl);
      encode(meta, bl);
      encode(rm_xattrs, bl);
      encode(epoch, bl);
      encode(version, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      DECODE_START_LEGACY_COMPAT_LEN(5, 3, 3, bl);
      decode(status, bl);
      decode(flags, bl);
      decode(data, bl);
      decode(xattrs, bl);
      decode(meta, bl);
      if (struct_v >= 2)
	decode(rm_xattrs, bl);
      if (struct_v >= 4)
	decode(epoch, bl);
      if (struct_v >= 5)
	decode(version, bl);
      DECODE_FINISH(bl);
    }
    void dump(Formatter *f) const;
    static void generate_test_instances(list<ObjectCacheInfo*>& o);
  };
WRITE_CLASS_ENCODER(ObjectCacheInfo)

  struct RGWCacheNotifyInfo {
    uint32_t op;
    rgw_raw_obj obj;
    ObjectCacheInfo obj_info;
    off_t ofs;
    string ns;

    RGWCacheNotifyInfo() : op(0), ofs(0) {}

    void encode(bufferlist& obl) const {
      ENCODE_START(2, 2, obl);
      encode(op, obl);
      encode(obj, obl);
      encode(obj_info, obl);
      encode(ofs, obl);
      encode(ns, obl);
      ENCODE_FINISH(obl);
    }
    void decode(bufferlist::const_iterator& ibl) {
      DECODE_START_LEGACY_COMPAT_LEN(2, 2, 2, ibl);
      decode(op, ibl);
      decode(obj, ibl);
      decode(obj_info, ibl);
      decode(ofs, ibl);
      decode(ns, ibl);
      DECODE_FINISH(ibl);
    }
    void dump(Formatter *f) const;
    static void generate_test_instances(list<RGWCacheNotifyInfo*>& o);
  };
WRITE_CLASS_ENCODER(RGWCacheNotifyInfo)

  class RGWChainedCache {
    public:
      virtual ~RGWChainedCache() {}
      virtual void chain_cb(const string& key, void *data) = 0;
      virtual void invalidate(const string& key) = 0;
      virtual void invalidate_all() = 0;
      virtual void unregistered() {}

      struct Entry {
	RGWChainedCache *cache;
	const string& key;
	void *data;

	Entry(RGWChainedCache *_c, const string& _k, void *_d) : cache(_c), key(_k), data(_d) {}
      };
  };


struct ObjectCacheEntry {
  ObjectCacheInfo info;
  std::list<string>::iterator lru_iter;
  uint64_t lru_promotion_ts;
  uint64_t gen;
  std::vector<pair<RGWChainedCache *, string> > chained_entries;

  ObjectCacheEntry() : lru_promotion_ts(0), gen(0) {}
};

class ObjectCache {
  std::unordered_map<string, ObjectCacheEntry> cache_map;
  std::list<string> lru;
  unsigned long lru_size;
  unsigned long lru_counter;
  unsigned long lru_window;
  ceph::shared_mutex lock = ceph::make_shared_mutex("ObjectCache");
  CephContext *cct;

  vector<RGWChainedCache *> chained_cache;

  bool enabled;
  ceph::timespan expiry;

  void touch_lru(const string& name, ObjectCacheEntry& entry,
      std::list<string>::iterator& lru_iter);
  void remove_lru(const string& name, std::list<string>::iterator& lru_iter);
  void invalidate_lru(ObjectCacheEntry& entry);

  void do_invalidate_all();

  public:
  ObjectCache() : lru_size(0), lru_counter(0), lru_window(0), cct(NULL), enabled(false) { }
  ~ObjectCache();
  int get(const std::string& name, ObjectCacheInfo& bl, uint32_t mask, rgw_cache_entry_info *cache_info);
  std::optional<ObjectCacheInfo> get(const std::string& name) {
    std::optional<ObjectCacheInfo> info{std::in_place};
    auto r = get(name, *info, 0, nullptr);
    return r < 0 ? std::nullopt : info;
  }

  template<typename F>
    void for_each(const F& f) {
      std::shared_lock l{lock};
      if (enabled) {
	auto now  = ceph::coarse_mono_clock::now();
	for (const auto& [name, entry] : cache_map) {
	  if (expiry.count() && (now - entry.info.time_added) < expiry) {
	    f(name, entry);
	  }
	}
      }
    }

  void put(const std::string& name, ObjectCacheInfo& bl, rgw_cache_entry_info *cache_info);
  bool remove(const std::string& name);
  void set_ctx(CephContext *_cct) {
    cct = _cct;
    lru_window = cct->_conf->rgw_cache_lru_size / 2;
    expiry = std::chrono::seconds(cct->_conf.get_val<uint64_t>(
	  "rgw_cache_expiry_interval"));
  }
  bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
      RGWChainedCache::Entry *chained_entry);

  void set_enabled(bool status);

  void chain_cache(RGWChainedCache *cache);
  void unchain_cache(RGWChainedCache *cache);
  void invalidate_all();
};

/* datacache */

class CacheThreadPool {
  public:
    CacheThreadPool(int n) {
      for (int i=0; i<n; ++i) {
	threads.push_back(new PoolWorkerThread(workQueue));
	threads.back()->start();
      }
    }
    ~CacheThreadPool() {
      finish();
    }

    void addTask(Task *nt) {
      workQueue.addTask(nt);
    }

    void finish() {
      for (size_t i=0,e=threads.size(); i<e; ++i)
	workQueue.addTask(NULL);
      for (size_t i=0,e=threads.size(); i<e; ++i) {
	threads[i]->join();
	delete threads[i];
      }
      threads.clear();
    }

  private:
    std::vector<PoolWorkerThread*> threads;
    WorkQueue workQueue;
};



class CopyRemoteS3Object : public Task {
   public:
    CopyRemoteS3Object(CephContext *_cct, RGWRados *_store, string _owner_obj, uint64_t _t_size, cache_obj *_c_obj, bool _coales_write, std::list<string>& _out_small_writes, RemoteRequest *_req) : Task(), cct(_cct), store(_store), owner_obj(_owner_obj), t_size(_t_size), c_obj(_c_obj), coales_write(_coales_write), out_small_writes(_out_small_writes), req(_req) {
	  // std::list<cache_obj*> *write_list = new std::list<cache_obj*>;
	  // outstanding_small_write_list = new std::list<cache_obj*>
      pthread_mutex_init(&qmtx,0);
      pthread_cond_init(&wcond, 0);
    }
    ~CopyRemoteS3Object() {
      pthread_mutex_destroy(&qmtx);
      pthread_cond_destroy(&wcond);
    }
    virtual void run();
	virtual void set_handler(void *handle) {
      curl_handle = (CURL *)handle;
    }
	  string sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
    string get_date();
 private:
    
	int submit_http_put_request_s3();
	int submit_http_put_coalesed_requests_s3();
	int submit_coalesing_writes_s3();

 private:
    CURL *curl_handle;
    pthread_mutex_t qmtx;
    pthread_cond_t wcond;
    CephContext *cct;
	RGWRados *store;
	string owner_obj;
	uint64_t t_size;
    cache_obj *c_obj;
	bool coales_write;
    std::list<string>& out_small_writes;
    RemoteRequest *req;
	bufferlist pbl;

};

class RemoteS3Request : public Task {
  public:
    RemoteS3Request(RemoteRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
      pthread_mutex_init(&qmtx,0);
      pthread_cond_init(&wcond, 0);
    }
    ~RemoteS3Request() {
      pthread_mutex_destroy(&qmtx);
      pthread_cond_destroy(&wcond);
    }
    virtual void run();
    virtual void set_handler(void *handle) {
      curl_handle = (CURL *)handle;
    }
    string sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
    string get_date();
  private:
    int submit_http_get_request_s3();
  private:
    pthread_mutex_t qmtx;
    pthread_cond_t wcond;
    RemoteRequest *req;
    CephContext *cct;
    CURL *curl_handle;

};

struct cacheAioWriteRequest{
  std::string key;
  void *data;
  int fd;
  struct aiocb *cb;
  DataCache *priv_data;
  CephContext *cct;
  bool write; 
  RGWRados *store;
  cache_block c_block;

  cacheAioWriteRequest(CephContext *_cct) : cct(_cct) , write(false) {}
  int create_io(bufferlist& bl, uint64_t len, std::string key);

  void release() {
    ::close(fd);
    cb->aio_buf = nullptr;
    free(data);
    data = nullptr;
    free(cb);
    free(this);
  }
};

struct ObjectDataInfo : public LRUObject {
  CephContext *cct;
  uint64_t size;
  string obj_id;
  bool complete;
  struct ObjectDataInfo *lru_prev;
  struct ObjectDataInfo *lru_next;
  cache_obj *c_obj;
  
  ObjectDataInfo(): size(0) {}
  void set_ctx(CephContext *_cct) {cct = _cct;}
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectDataInfo*>& o);
};

struct ChunkDataInfo : public LRUObject {
  CephContext *cct;
  uint64_t size;
  time_t access_time;
  string address;
  string obj_id;
  bool complete;
  struct ChunkDataInfo *lru_prev;
  struct ChunkDataInfo *lru_next;

  ChunkDataInfo(): size(0) {}
  void set_ctx(CephContext *_cct) {cct = _cct;}
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ChunkDataInfo*>& o);
};

struct DataCache {
  private:
	std::vector<string> remote_cache_list;
	int remote_cache_count;
    std::map<string, ObjectDataInfo*> write_cache_map;
    std::map<string, ChunkDataInfo*> cache_map;
    std::list<string> outstanding_write_list;
    std::list<string> *small_writes;
	std::list<cache_obj*> *outstanding_small_write_list;
	uint64_t total_write_size = 0;
	CephContext *cct;
    std::string path;
    uint64_t free_data_cache_size;
    uint64_t outstanding_write_size;
    CacheThreadPool *tp;
    CacheThreadPool *aging_tp;
    ceph::mutex obj_cache_lock = ceph::make_mutex("DataCache::obj_cache_lock");
    ceph::mutex age_cache_lock = ceph::make_mutex("DataCache::age_cache_lock");
    ceph::mutex cache_lock = ceph::make_mutex("DataCache::cache_lock");
    ceph::mutex eviction_lock = ceph::make_mutex("DataCache::eviction_lock");
    RGWBlockDirectory *blkDirectory;
    RGWObjectDirectory *objDirectory;
	int local_hit ;
	int remote_hit;
	int datalake_hit;

    struct ChunkDataInfo *head;
    struct ChunkDataInfo *tail;
    struct ObjectDataInfo *obj_head;
    struct ObjectDataInfo *obj_tail;
    std::atomic<std::uint32_t> uid { 0 };  // <<== initialised

  public:
    DataCache() ;
    ~DataCache() {}
    void retrieve_block_info(cache_block* c_block, RGWRados *store);
    void submit_remote_req(struct RemoteRequest *c);
    size_t lru_eviction();
	int evict_from_directory(string key);
	void put(bufferlist& bl, uint64_t len, string obj_id, cache_block* c_block);
	void put_obj(cache_obj* c_obj);
    bool get(string oid);
	int create_aio_write_request(bufferlist& bl, uint64_t len, std::string obj_id, cache_block* c_block);
    void cache_aio_write_completion_cb(cacheAioWriteRequest *c);
    size_t get_used_pool_capacity(string pool_name, RGWRados *store);
    void copy_aged_obj(RGWRados *store, uint64_t interval);
	void init_writecache_aging(RGWRados *store);
    void timer_start(RGWRados *store, uint64_t interval);
	string sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
	int getUid();
	string get_date();
	int submit_http_head_requests_s3(cache_obj *c_obj);
	int submit_http_get_requests_s3(cache_obj *c_obj, string prefix, string marker, int max_b);
	void set_remote_cache_list();
	void init(CephContext *_cct) {
      cct = _cct;
      free_data_cache_size = cct->_conf->rgw_cache_size;
      path = cct->_conf->rgw_datacache_path;
      tp = new CacheThreadPool(cct->_conf->cache_threadpool_size);
      aging_tp = new CacheThreadPool(cct->_conf->cache_aging_threadpool_size);
      head = nullptr;
      tail = nullptr;
      obj_head = nullptr;
      obj_tail = nullptr;
      outstanding_small_write_list = new std::list<cache_obj*>;
      small_writes = new std::list<string>;
	  set_remote_cache_list();
	  local_hit = 0;
	  remote_hit = 0;
	  datalake_hit = 0;
	}

	void increase_remote_hit(){
	   cache_lock.lock();
	   remote_hit ++;
	   cache_lock.unlock();
	}
    void set_block_directory(RGWBlockDirectory *_blkDirectory){
	blkDirectory = _blkDirectory;
    }
    void set_object_directory(RGWObjectDirectory *_objDirectory){
	objDirectory = _objDirectory;
    }

	 
	void lru_insert_head(struct ChunkDataInfo *o) {
        o->lru_next = head;
        o->lru_prev = nullptr;
        if (head) {
                head->lru_prev = o;
        } else {
                tail = o;
        }
        head = o;
    }
    void lru_insert_tail(struct ChunkDataInfo *o) {
        o->lru_next = nullptr;
        o->lru_prev = tail;
        if (tail) {
                tail->lru_next = o;
        } else {
                head = o;
        }
        tail = o;
    }
    void lru_remove(struct ChunkDataInfo *o) {
        if (o->lru_next)
                o->lru_next->lru_prev = o->lru_prev;
        else
                tail = o->lru_prev;
        if (o->lru_prev)
                o->lru_prev->lru_next = o->lru_next;
        else
                head = o->lru_next;
        o->lru_next = o->lru_prev = nullptr;
     }

	  void obj_lru_insert_head(struct ObjectDataInfo *o) {
        o->lru_next = obj_head;
        o->lru_prev = nullptr;
        if (obj_head) {
                obj_head->lru_prev = o;
        } else {
                obj_tail = o;
        }
        obj_head = o;
    }
    void obj_lru_remove(struct ObjectDataInfo *o) {
        if (o->lru_next)
                o->lru_next->lru_prev = o->lru_prev;
        else
                obj_tail = o->lru_prev;
        if (o->lru_prev)
                o->lru_prev->lru_next = o->lru_next;
        else
                obj_head = o->lru_next;
        o->lru_next = o->lru_prev = nullptr;
     }

};

#endif

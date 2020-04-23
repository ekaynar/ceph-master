// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef _RGW_DATACACHE_
#define _RGW_DATACACHE_

#include <string>
#include <map>
#include <unordered_map>
#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"

#include "cls/version/cls_version_types.h"
#include "rgw_common.h"
#include <aio.h>

//class DataCache;
/*
struct ChunkDataInfo {
  CephContext *cct;
  uint64_t size;
  time_t access_time;
  string oid;
  bool complete;

  ChunkDataInfo(): size(0) {}

  void set_ctx(CephContext *_cct) {
    cct = _cct;
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ChunkDataInfo*>& o);
};

struct cacheAioWriteRequest{
  string key;
  void *data;
  int fd;
  struct aiocb *cb;
  DataCache *priv_data;
  CephContext *cct;
  bool write;

  cacheAioWriteRequest(CephContext *_cct) : cct(_cct) , write(false) {}
  int create_io(bufferlist& bl, uint64_t len, string key);

  void release() {
    ::close(fd);
    cb->aio_buf = NULL;
    free(data);
    data = NULL;
    free(cb);
    free(this);
  }
};

*/

struct DataCache{
private:
  std::list<string> outstanding_write_list;
  uint64_t free_data_cache_size;
  //CephContext *cct;
  enum _io_type {
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;
public:
  //DataCache() : free_data_cache_size(0) {}
  //~DataCache();
  void put(bufferlist& bl, uint64_t len, string obj_key, CephContext *cct);
  /*void set_ctx(CephContext *_cct){
    cct = _cct;
  }*/
  void init(){
    free_data_cache_size = 10;
  } 

};
/*
struct DataCache {
private:
  std::list<string> outstanding_write_list;
  int index;
  ceph::shared_mutex lock;
  ceph::mutex  cache_lock;
  ceph::mutex  req_lock;
  ceph::mutex  eviction_lock;
  CephContext *cct;
  enum _io_type {
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;

  struct sigaction action;
  uint64_t free_data_cache_size = 0;
  uint64_t outstanding_write_size;

public:
  DataCache();
  ~DataCache() {};
  void put(bufferlist& bl, uint64_t len, string obj_key);
  int create_aio_write_request(bufferlist& bl, uint64_t len, std::string key);
  void cache_aio_write_completion_cb(cacheAioWriteRequest *c);
  void init(CephContext *_cct) {
    cct = _cct;
    free_data_cache_size = 1000;
  }
};
*/

#endif

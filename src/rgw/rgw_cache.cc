// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_cache.h"
  uint64_t expected_size = 0;
#include "rgw_perf_counters.h"

#include <errno.h>

#define dout_subsys ceph_subsys_rgw

/*datacache*/
#include <iostream>
#include <fstream> // writing to file
#include <sstream> // writing to memory (a string)
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include "rgw_rest_conn.h"
//#include "rgw_cacherequest.h"

int ObjectCache::get(const string& name, ObjectCacheInfo& info, uint32_t mask, rgw_cache_entry_info *cache_info)
{

  std::shared_lock rl{lock};
  if (!enabled) {
    return -ENOENT;
  }
  auto iter = cache_map.find(name);
  if (iter == cache_map.end()) {
    ldout(cct, 10) << "cache get: name=" << name << " : miss" << dendl;
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  if (expiry.count() &&
      (ceph::coarse_mono_clock::now() - iter->second.info.time_added) > expiry) {
    ldout(cct, 10) << "cache get: name=" << name << " : expiry miss" << dendl;
    rl.unlock();
    std::unique_lock wl{lock};  // write lock for insertion
    // check that wasn't already removed by other thread
    iter = cache_map.find(name);
    if (iter != cache_map.end()) {
      for (auto &kv : iter->second.chained_entries)
	kv.first->invalidate(kv.second);
      remove_lru(name, iter->second.lru_iter);
      cache_map.erase(iter);
    }
    if (perfcounter) {
      perfcounter->inc(l_rgw_cache_miss);
    }
    return -ENOENT;
  }

  ObjectCacheEntry *entry = &iter->second;

  if (lru_counter - entry->lru_promotion_ts > lru_window) {
    ldout(cct, 20) << "cache get: touching lru, lru_counter=" << lru_counter
      << " promotion_ts=" << entry->lru_promotion_ts << dendl;
    rl.unlock();
    std::unique_lock wl{lock};  // write lock for insertion
    /* need to redo this because entry might have dropped off the cache */
    iter = cache_map.find(name);
    if (iter == cache_map.end()) {
      ldout(cct, 10) << "lost race! cache get: name=" << name << " : miss" << dendl;
      if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
      return -ENOENT;
    }

    entry = &iter->second;
    /* check again, we might have lost a race here */
    if (lru_counter - entry->lru_promotion_ts > lru_window) {
      touch_lru(name, *entry, iter->second.lru_iter);
    }
  }

  ObjectCacheInfo& src = iter->second.info;
  if ((src.flags & mask) != mask) {
    ldout(cct, 10) << "cache get: name=" << name << " : type miss (requested=0x"
      << std::hex << mask << ", cached=0x" << src.flags
      << std::dec << ")" << dendl;
    if(perfcounter) perfcounter->inc(l_rgw_cache_miss);
    return -ENOENT;
  }
  ldout(cct, 10) << "cache get: name=" << name << " : hit (requested=0x"
    << std::hex << mask << ", cached=0x" << src.flags
    << std::dec << ")" << dendl;

  info = src;
  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry->gen;
  }
  if(perfcounter) perfcounter->inc(l_rgw_cache_hit);

  return 0;
}

bool ObjectCache::chain_cache_entry(std::initializer_list<rgw_cache_entry_info*> cache_info_entries,
    RGWChainedCache::Entry *chained_entry)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  std::vector<ObjectCacheEntry*> entries;
  entries.reserve(cache_info_entries.size());
  /* first verify that all entries are still valid */
  for (auto cache_info : cache_info_entries) {
    ldout(cct, 10) << "chain_cache_entry: cache_locator="
      << cache_info->cache_locator << dendl;
    auto iter = cache_map.find(cache_info->cache_locator);
    if (iter == cache_map.end()) {
      ldout(cct, 20) << "chain_cache_entry: couldn't find cache locator" << dendl;
      return false;
    }

    auto entry = &iter->second;

    if (entry->gen != cache_info->gen) {
      ldout(cct, 20) << "chain_cache_entry: entry.gen (" << entry->gen
	<< ") != cache_info.gen (" << cache_info->gen << ")"
	<< dendl;
      return false;
    }
    entries.push_back(entry);
  }


  chained_entry->cache->chain_cb(chained_entry->key, chained_entry->data);

  for (auto entry : entries) {
    entry->chained_entries.push_back(make_pair(chained_entry->cache,
	  chained_entry->key));
  }

  return true;
}

void ObjectCache::put(const string& name, ObjectCacheInfo& info, rgw_cache_entry_info *cache_info)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return;
  }

  ldout(cct, 10) << "cache put: name=" << name << " info.flags=0x"
    << std::hex << info.flags << std::dec << dendl;

  auto [iter, inserted] = cache_map.emplace(name, ObjectCacheEntry{});
  ObjectCacheEntry& entry = iter->second;
  entry.info.time_added = ceph::coarse_mono_clock::now();
  if (inserted) {
    entry.lru_iter = lru.end();
  }
  ObjectCacheInfo& target = entry.info;

  invalidate_lru(entry);

  entry.chained_entries.clear();
  entry.gen++;

  touch_lru(name, entry, entry.lru_iter);

  target.status = info.status;

  if (info.status < 0) {
    target.flags = 0;
    target.xattrs.clear();
    target.data.clear();
    return;
  }

  if (cache_info) {
    cache_info->cache_locator = name;
    cache_info->gen = entry.gen;
  }

  target.flags |= info.flags;

  if (info.flags & CACHE_FLAG_META)
    target.meta = info.meta;
  else if (!(info.flags & CACHE_FLAG_MODIFY_XATTRS))
    target.flags &= ~CACHE_FLAG_META; // non-meta change should reset meta

  if (info.flags & CACHE_FLAG_XATTRS) {
    target.xattrs = info.xattrs;
    map<string, bufferlist>::iterator iter;
    for (iter = target.xattrs.begin(); iter != target.xattrs.end(); ++iter) {
      ldout(cct, 10) << "updating xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
    }
  } else if (info.flags & CACHE_FLAG_MODIFY_XATTRS) {
    map<string, bufferlist>::iterator iter;
    for (iter = info.rm_xattrs.begin(); iter != info.rm_xattrs.end(); ++iter) {
      ldout(cct, 10) << "removing xattr: name=" << iter->first << dendl;
      target.xattrs.erase(iter->first);
    }
    for (iter = info.xattrs.begin(); iter != info.xattrs.end(); ++iter) {
      ldout(cct, 10) << "appending xattr: name=" << iter->first << " bl.length()=" << iter->second.length() << dendl;
      target.xattrs[iter->first] = iter->second;
    }
  }

  if (info.flags & CACHE_FLAG_DATA)
    target.data = info.data;

  if (info.flags & CACHE_FLAG_OBJV)
    target.version = info.version;
}

bool ObjectCache::remove(const string& name)
{
  std::unique_lock l{lock};

  if (!enabled) {
    return false;
  }

  auto iter = cache_map.find(name);
  if (iter == cache_map.end())
    return false;

  ldout(cct, 10) << "removing " << name << " from cache" << dendl;
  ObjectCacheEntry& entry = iter->second;

  for (auto& kv : entry.chained_entries) {
    kv.first->invalidate(kv.second);
  }

  remove_lru(name, iter->second.lru_iter);
  cache_map.erase(iter);
  return true;
}

void ObjectCache::touch_lru(const string& name, ObjectCacheEntry& entry,
    std::list<string>::iterator& lru_iter)
{
  while (lru_size > (size_t)cct->_conf->rgw_cache_lru_size) {
    auto iter = lru.begin();
    if ((*iter).compare(name) == 0) {
      /*
       * if the entry we're touching happens to be at the lru end, don't remove it,
       * lru shrinking can wait for next time
       */
      break;
    }
    auto map_iter = cache_map.find(*iter);
    ldout(cct, 10) << "removing entry: name=" << *iter << " from cache LRU" << dendl;
    if (map_iter != cache_map.end()) {
      ObjectCacheEntry& entry = map_iter->second;
      invalidate_lru(entry);
      cache_map.erase(map_iter);
    }
    lru.pop_front();
    lru_size--;
  }

  if (lru_iter == lru.end()) {
    lru.push_back(name);
    lru_size++;
    lru_iter--;
    ldout(cct, 10) << "adding " << name << " to cache LRU end" << dendl;
  } else {
    ldout(cct, 10) << "moving " << name << " to cache LRU end" << dendl;
    lru.erase(lru_iter);
    lru.push_back(name);
    lru_iter = lru.end();
    --lru_iter;
  }

  lru_counter++;
  entry.lru_promotion_ts = lru_counter;
}

void ObjectCache::remove_lru(const string& name,
    std::list<string>::iterator& lru_iter)
{
  if (lru_iter == lru.end())
    return;

  lru.erase(lru_iter);
  lru_size--;
  lru_iter = lru.end();
}

void ObjectCache::invalidate_lru(ObjectCacheEntry& entry)
{
  for (auto iter = entry.chained_entries.begin();
      iter != entry.chained_entries.end(); ++iter) {
    RGWChainedCache *chained_cache = iter->first;
    chained_cache->invalidate(iter->second);
  }
}

void ObjectCache::set_enabled(bool status)
{
  std::unique_lock l{lock};

  enabled = status;

  if (!enabled) {
    do_invalidate_all();
  }
}

void ObjectCache::invalidate_all()
{
  std::unique_lock l{lock};

  do_invalidate_all();
}

void ObjectCache::do_invalidate_all()
{
  cache_map.clear();
  lru.clear();

  lru_size = 0;
  lru_counter = 0;
  lru_window = 0;

  for (auto& cache : chained_cache) {
    cache->invalidate_all();
  }
}

void ObjectCache::chain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};
  chained_cache.push_back(cache);
}

void ObjectCache::unchain_cache(RGWChainedCache *cache) {
  std::unique_lock l{lock};

  auto iter = chained_cache.begin();
  for (; iter != chained_cache.end(); ++iter) {
    if (cache == *iter) {
      chained_cache.erase(iter);
      cache->unregistered();
      return;
    }
  }
}

ObjectCache::~ObjectCache()
{
  for (auto cache : chained_cache) {
    cache->unregistered();
  }
}


/* datacache */

DataCache::DataCache () : cct(NULL), capacity(0)  {}

void DataCache::submit_remote_req(struct RemoteRequest *c){
    ldout(cct, 0) << "submit_remote_req" <<dendl;
    tp->addTask(new RemoteS3Request(c, cct));
}

void DataCache::retrieve_obj_info(cache_obj& c_obj){
    ldout(cct, 0) << __func__ <<dendl;
    ldout(cct, 0) << c_obj.bucket_name <<dendl;
    c_obj.loc = 2;

}

int cacheAioWriteRequest::create_io(bufferlist& bl, uint64_t len, string key) {
  //std::string location =  "/tmp/"+ key;
  std::string location = cct->_conf->rgw_datacache_path + "/"+ key;
  int ret = 0;
  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  ret = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0)
  {
    ldout(cct, 0) << "ERROR: create_aio_write_request: open file failed, " << errno << "\tlocation: " << location.c_str() <<dendl;
    goto done;
  }
  cb->aio_fildes = fd;

  data = malloc(len);
  if(!data)
  {
    ldout(cct, 0) << "ERROR: create_aio_write_request: memory allocation failed" << dendl;
    goto close_file;
  }
  cb->aio_buf = data;
  memcpy((void *)data, bl.c_str(), len);
  cb->aio_nbytes = len;
  goto done;	
close_file:
  ::close(fd);
done:
  ldout(cct, 0) << "done" << dendl;
  return ret;
}


void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest *c){
  //ldout(cct, 0) << "engage: cache_aio_write_completion_cb oid:" <<dendl;
  //outstanding_write_list.remove(c->key);
  c->release(); 

}


void _cache_aio_write_completion_cb(sigval_t sigval) {
  cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
  c->priv_data->cache_aio_write_completion_cb(c);
}

int DataCache::create_aio_write_request(bufferlist& bl, uint64_t len, std::string key){
  struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest(cct);
  int ret = 0;
  if (wr->create_io(bl, len, key) < 0) {
    ldout(cct, 0) << "Error: create_io " << dendl;
    goto done;
  }

  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = _cache_aio_write_completion_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = NULL;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
  wr->key = key;
  wr->priv_data = this;
  if((ret= ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "Error: aio_write failed "<< ret << dendl;
    goto error;
  }
  //  ldout(cct, 10) << "after error1" << dendl;
  return 0;

error:
  wr->release();
done:
  return ret;


}
void DataCache::put(bufferlist& bl, uint64_t len, string key){
  ldout(cct, 10) << __func__  << key <<dendl;
  int ret = 0;
  // cache_lock.lock();  
  std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),key);
  if (it != outstanding_write_list.end()) {
    //       cache_lock.unlock();
    ldout(cct, 5) << "re-write: write already issued, key "<< key << dendl;
    return;
  }
  outstanding_write_list.push_back(key);
  //cache_lock.unlock();

  ret = create_aio_write_request(bl, len, key);
  if (ret < 0) {
    //  cache_lock.lock();
    outstanding_write_list.remove(key);
    //    cache_lock.unlock();
    ldout(cct, 1) << "Error: create_aio_write_request failed "  << ret << dendl;
    return;
  }
}

/*Remote S3 Request datacacahe*/
int RemoteS3Request::submit_op() {
  ldout(cct, 10) << __func__  << " for block" <<  req->key << dendl;
  real_time set_mtime;
  uint64_t expected_size = 0;
  bool prepend_metadata = true; 
  bool rgwx_stat = false;
  bool skip_decrypt =false;
  bool get_op = true;
  bool sync_manifest =false;
  bool send = true;
  string user="testuser";
  int ret = req->conn->get_obj(user, req->ofs, req->read_len, req->obj, prepend_metadata,
		    get_op, rgwx_stat, sync_manifest, skip_decrypt, send, req->cb, &req->in_stream_req);
  ldout(cct, 10) << __func__  <<"after submit_op get_obj" << ret << " " << req->ofs << dendl;
  if (ret < 0 )
    return ret;
  ret = req->conn->complete_request(req->in_stream_req, nullptr, &set_mtime, &expected_size, nullptr, nullptr);
  if (ret < 0 ){
     ldout(cct, 10) << __func__  <<"not complete_reque ugur" << dendl;
  req->r->result = -1;
  req->aio->put(*(req->r));
    return ret;
  }
  //ldout(cct, 10) << __func__  << etag << dendl;
  ldout(cct, 10) << __func__  <<"completed" << dendl;
  ldout(cct, 10) << __func__  <<req->ofs << dendl;
  req->r->result = 0;
  req->aio->put(*(req->r));
  return 0;
}

void RemoteS3Request::run() {

  ldout(cct, 10) << __func__  <<dendl;
  //int retries =  cct->_conf->rgw_l2_request_thread_num;   
  int retries = 10;
  int r = 0;
  for (int i=0; i<retries; i++ ){
    if(!(r = submit_op())){
	ldout(cct, 10) <<" after run func"  <<dendl;
      //req->cb->bl = std::move(req->cb->pbl);
      //req->r->result = 0;
      //req->aio->put(*(req->r));
      req->finish();
      return;
    }
    ldout(cct, 10) <<"failed"<< req->key  <<dendl;
    }


}



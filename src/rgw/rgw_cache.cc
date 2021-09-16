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
#include <openssl/hmac.h>
#include <ctime>
#include <mutex>
#include <curl/curl.h>
#include <time.h>
#include <algorithm>
#include <string>
//#include "rgw_cacherequest.h"
static const std::string base64_chars =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";


static inline bool is_base64(unsigned char c) {
  return (isalnum(c) || (c == '+') || (c == '/'));
}
std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];
  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
	ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';
  }
  return ret;
}






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

DataCache::DataCache() : cct(NULL), free_data_cache_size(0), outstanding_write_size (0){}

void DataCache::submit_remote_req(RemoteRequest *c){
  string endpoint=cct->_conf->backend_url;
  cache_lock.lock();

  ldout(cct, 0) << "submit_remote_req, dest " << c->dest << " endpoint "<< endpoint<< dendl;
  if ((c->dest).compare(endpoint) == 0) {
        datalake_hit ++;
	    ldout(cct, 0) << "submit_remote_req, datalake_hit " << datalake_hit<< dendl;
  } else {
        remote_hit++;
	    ldout(cct, 0) << "submit_remote_req, remote_hit " <<  remote_hit<< dendl;
  }
  cache_lock.unlock();

  tp->addTask(new RemoteS3Request(c, cct));
}

void DataCache::retrieve_block_info(cache_block* c_block, RGWRados *store){
  ldout(cct, 0) << __func__ <<dendl;
  int ret = store->blkDirectory->getValue(c_block);
}


void DataCache::copy_aged_obj(RGWRados *store, uint64_t interval){
	ldout(cct, 20) << __func__ << dendl;
	time_t rawTime = time(NULL);
    time_t now =  mktime(gmtime(&rawTime));
	obj_cache_lock.lock();
    ObjectDataInfo *del_entry;
    del_entry = obj_tail;
	obj_cache_lock.unlock();

	while (obj_head != NULL and del_entry != NULL) {
	  
	  ldout(cct, 20) << __func__ << del_entry->c_obj->obj_name <<dendl;
	  double seconds = difftime(now,del_entry->c_obj->creationTime);
	  if (seconds < (double(interval)*60))
		break;
	 
	  cache_obj *c_obj = new cache_obj();
      c_obj->bucket_name = del_entry->c_obj->bucket_name;
      c_obj->obj_name = del_entry->c_obj->obj_name;
	  
	  // Check Object exists or not
	  if ( !store->get_obj(c_obj)){
		obj_cache_lock.lock();
		string obj_id = del_entry->obj_id;
		ObjectDataInfo *aged_item;
        aged_item = del_entry;
		map<string, ObjectDataInfo*>::iterator iter = write_cache_map.find(obj_id);
		if (iter != write_cache_map.end())
          write_cache_map.erase(obj_id);
		  obj_lru_remove(aged_item);
		  obj_cache_lock.unlock();
		  del_entry = obj_tail;
	  }

	  // Check the object is intermediate data or not
	  c_obj->bucket_name = del_entry->c_obj->bucket_name;
	  c_obj->obj_name = del_entry->c_obj->obj_name;
	  c_obj->size_in_bytes = del_entry->c_obj->size_in_bytes;
	  c_obj->owner = del_entry->c_obj->owner;
	  string owner_obj =  del_entry->c_obj->owner;
	  
	   
	  int ret = objDirectory->getValue(c_obj);
      // Don't age if it is intermediate
      if (objDirectory->getValue(c_obj) == 0  && c_obj->intermediate == true){
        //obj_cache_lock.lock();
		del_entry = del_entry->lru_prev;
		ldout(cct, 20) << __func__ << "inter" <<del_entry->c_obj->obj_name <<dendl;
        //obj_lru_insert_head(del_entry);
        //obj_cache_lock.unlock();
	//small object coalesing writes
	 } else if (c_obj->size_in_bytes < cct->_conf->rgw_obj_stripe_size and cct->_conf->enable_coalesing_write){
		string obj_id = del_entry->obj_id;
		obj_cache_lock.lock();
		total_write_size += c_obj->size_in_bytes;
		small_writes->push_back(obj_id+"_"+std::to_string(c_obj->size_in_bytes));
		ObjectDataInfo *aged_item;
        aged_item = del_entry;
		map<string, ObjectDataInfo*>::iterator iter = write_cache_map.find(obj_id);
		if (iter != write_cache_map.end())
		  write_cache_map.erase(obj_id);

	    obj_lru_remove(aged_item);
		obj_cache_lock.unlock();
		del_entry = obj_tail;
		
		if (total_write_size >=  cct->_conf->coalesing_write_size){
		  std::list<string> *outgoing_small_writes = new std::list<string>;
		  obj_cache_lock.lock();
		  while(!small_writes->empty()){
		  string aging_candidate = small_writes->front();
		  outgoing_small_writes->push_back(aging_candidate);
		  small_writes->pop_front();
		  }
		  uint64_t t_size = total_write_size;
		  total_write_size = 0;
		  obj_cache_lock.unlock();
		  RemoteRequest *c =  new RemoteRequest();
		  aging_tp->addTask(new CopyRemoteS3Object(this->cct, store, owner_obj ,t_size, c_obj, true, *outgoing_small_writes, c));
		  }
	  
	  
	  } else {
		ldout(cct, 20) << __func__ << " Large Object" <<dendl;
		string obj_id = del_entry->obj_id;
		ObjectDataInfo *aged_item;
		aged_item = del_entry;
		del_entry = del_entry->lru_prev;
		obj_cache_lock.lock();
		map<string, ObjectDataInfo*>::iterator iter = write_cache_map.find(obj_id);
        if (iter != write_cache_map.end())
          write_cache_map.erase(obj_id);
        obj_lru_remove(aged_item);
        obj_cache_lock.unlock();
		uint64_t t_size = c_obj->size_in_bytes;
		RemoteRequest *c =  new RemoteRequest();
		std::list<string> *outgoing_small_writes = new std::list<string>;
		outgoing_small_writes->push_back(obj_id+"_"+std::to_string(c_obj->size_in_bytes));
		aging_tp->addTask(new CopyRemoteS3Object(this->cct, store, owner_obj ,t_size, c_obj, false, *outgoing_small_writes, c));

	  }
	  
	  
	  
	  /*
	   else {
		ldout(cct, 20) << __func__ << " aging large objects" <<dendl;
		string del_oid = del_entry->obj_id;
	    time_t now = time(NULL);
	   double diff = difftime(now, del_entry->c_obj->creationTime);
		if (diff < double(interval*60)) {
		  std::list<string> outgoing_small_writes;
		  string owner = c_obj->owner;
		  uint64_t t_size = c_obj->size_in_bytes;
		  RemoteRequest *c =  new RemoteRequest();
		  aging_tp->addTask(new CopyRemoteS3Object(this->cct, store, owner, t_size,  c_obj, false, outgoing_small_writes,c));
			
		  obj_cache_lock.lock();
		  map<string, ObjectDataInfo*>::iterator iter = write_cache_map.find(del_oid);
		  if (iter != write_cache_map.end()){
			write_cache_map.erase(del_oid);
		  }
		  obj_lru_remove(del_entry);
		  obj_cache_lock.unlock(); 
		}
		else { break; }
	  }
	  */
  }
  //ldout(cct, 20) << __func__ << "out of while loop "<< dendl;
}



void DataCache::timer_start(RGWRados *store, uint64_t interval)
{
  ldout(cct, 20) << __func__ << dendl;
  std::thread([store, interval, this]() {
  while(true){
	this->copy_aged_obj(store, interval);
//	std::this_thread::sleep_for(std::chrono::seconds(30));
	std::this_thread::sleep_for(std::chrono::minutes(interval));
  }
  }).detach();
  
}

int DataCache::getUid() {
    return ++uid;
}

int CopyRemoteS3Object::submit_http_put_request_s3(){
  int ret = store->copy_remote(store, c_obj);
  return ret;
}

int CopyRemoteS3Object::submit_coalesing_writes_s3(){
  int ret =  store->copy_small_remote(store, owner_obj, t_size, c_obj, out_small_writes, pbl);
  return ret;
}

void CopyRemoteS3Object::run() {
  int max_retries = cct->_conf->max_aging_retries;
  int ret = 0;
  for (int i=0; i<max_retries; i++ ){
	if(coales_write){
	  if(!(ret = submit_http_put_coalesed_requests_s3())){
	   return;	
	  }
	  ldout(cct, 0) << "ERROR: " << __func__  << "submit_http_put_coalesed_requests_s3() failed" << dendl;
      return;
	  }
	
	
	else {
	  if(!(ret = submit_http_put_request_s3())){
		ret = store->delete_writecache_obj(store, c_obj);

		return;
	  }
	  ldout(cct, 0) << "ERROR: " << __func__  << "submit_http_put_request_s3() failed" << dendl;
	  return;
	}
  }
}

void DataCache::init_writecache_aging(RGWRados *store){
  ldout(cct, 0) << __func__ <<dendl;
  
  timer_start(store, cct->_conf->aging_interval_in_minutes);

}

size_t DataCache::get_used_pool_capacity(string pool_name, RGWRados *store){

  ldout(cct, 0) << __func__ <<dendl;
  size_t used_capacity = 0;
/*
  librados::Rados *rados = store->get_rados_handle();
  list<string> vec;
  int r = rados->pool_list(vec);
  //  vec.push_back(pool_name);
  map<string,librados::pool_stat_t> stats;
  r = rados->get_pool_stats(vec, stats);
  if (r < 0) {
    ldout(cct, 0) << "error fetching pool stats: " <<dendl;
    return -1;
  }
  for (map<string,librados::pool_stat_t>::iterator i = stats.begin();i != stats.end(); ++i) {
    const char *pool_name = i->first.c_str();
    librados::pool_stat_t& s = i->second;
  }*/
  return used_capacity; //return used size
}


int cacheAioWriteRequest::create_io(bufferlist& bl, uint64_t len, string key) {
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


void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest* c){
  ChunkDataInfo  *chunk_info = nullptr;
  
  ldout(cct, 10) << __func__  << " oid " << c->key << dendl; 

  cache_lock.lock();
  /*auto it = outstanding_write_list.find(c->key);
    if (it != outstanding_write_list.end()) {
      outstanding_write_list.erase(it);
  }*/
  outstanding_write_list.remove(c->key);
  chunk_info = new ChunkDataInfo;
  chunk_info->obj_id = c->key;
  chunk_info->set_ctx(cct);
  chunk_info->size = c->cb->aio_nbytes;
  cache_map.insert(pair<string, ChunkDataInfo*>(c->key, chunk_info));
  cache_lock.unlock();
  
  /*update free size*/
  eviction_lock.lock();
  free_data_cache_size -= c->cb->aio_nbytes;
  outstanding_write_size -=  c->cb->aio_nbytes;
  lru_insert_head(chunk_info);
  eviction_lock.unlock();
  
  time_t rawTime = time(NULL);
  c->c_block.lastAccessTime = mktime(gmtime(&rawTime));  
  c->c_block.access_count = 0;
  int ret = blkDirectory->setValue(&(c->c_block));
  ldout(cct, 20) << __func__ <<"key done:" <<c->key << " ret:"<< ret <<dendl; 
  //delete c;
  //c = nullptr;
  c->release(); 

}


void _cache_aio_write_completion_cb(sigval_t sigval) {
  cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
  c->priv_data->cache_aio_write_completion_cb(c);
}

int DataCache::create_aio_write_request(bufferlist& bl, uint64_t len, std::string key, cache_block *c_b){
  ldout(cct, 10) << __func__  << " oid " << c_b->c_obj.obj_name <<  " blockid "<<  c_b->block_id <<dendl;
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
  wr->c_block = *c_b;

  if((ret= ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "Error: aio_write failed "<< ret << dendl;
    goto error;
  }
  return 0;

error:
  wr->release();
done:
  return ret;


}
bool DataCache::get(string oid) {

  string key = oid;
  const char x = '/';
  const char y = '_';
  std::replace(key.begin(), key.end(), x, y);

  ldout(cct, 0) << __func__ << "key:"<< key << dendl;
  bool exist = false;
  int ret = 0;
  string location = cct->_conf->rgw_datacache_path + "/"+ key;
  cache_lock.lock();
  map<string, ChunkDataInfo*>::iterator iter = cache_map.find(key);
  if (!(iter == cache_map.end())){
     // check inside cache whether file exists or not!!!! then make exist true;
     struct ChunkDataInfo *chdo = iter->second;
     if(access(location.c_str(), F_OK ) != -1 ) { // file exists
 	  exist = true;
 	  /* LRU */
 	  eviction_lock.lock();
 	  lru_remove(chdo);
 	  lru_insert_head(chdo);
	  local_hit += 1;
	  ldout(cct, 0) << __func__ << " filexist:"<< key << " local_hit " << local_hit << dendl;
 	  eviction_lock.unlock();
     } 
	 else { /*LRU*/
	  ldout(cct, 0) << __func__ << " in map but file not exist:"<< key << dendl;
	  ret = evict_from_directory(oid);
	  cache_map.erase(key);
 	  eviction_lock.lock();
 	  free_data_cache_size += chdo->size;
	  lru_remove(chdo);
      delete chdo;
	  exist = false;
 	  eviction_lock.unlock();
     }
  }
  else{
  ldout(cct, 0) << __func__ << "key not in map:"<< key << dendl;
  }
  cache_lock.unlock();
  return exist;
}

int DataCache::evict_from_directory(string key){
   /*update directory*/
    string hosts;
    int ret = blkDirectory->getHosts(key, hosts);
    stringstream sloction(hosts);
    string tmp;
    stringstream ss;
    vector<string> hosts_list;
    size_t i = 0;
    while(getline(sloction, tmp, '_')){
      if (tmp != cct->_conf->remote_cache_addr){
        hosts_list.push_back(tmp);
        if(i != 0)
          ss << "_";
        ss << tmp;
        i+=1;
      }
    }
    hosts = ss.str();
	if (hosts_list.size() <= 0){
      ret = blkDirectory->delValue(key);
    } else {
      ret = blkDirectory->updateField(key, "host_list", hosts);
    }
	return 0;
}

void DataCache::set_remote_cache_list(){
      remote_cache_count = 0;
          stringstream sloction(cct->_conf->remote_cache_list);
      string tmp;
      while(getline(sloction, tmp, ',')){
        if (tmp.compare(cct->_conf->remote_cache_addr) != 0)
                {
          remote_cache_list.push_back(tmp);
                  //remote_cache_weight_map.insert(pair<string, int>(tmp, 0));
                }
      }
          remote_cache_count = remote_cache_list.size();
    }


size_t DataCache::lru_eviction(){

  int n_entries = 0;
  size_t freed_size = 0;
  ChunkDataInfo *del_entry;
  string del_oid, location;

  n_entries = cache_map.size();
  ldout(cct, 10) << __func__  <<" del_id1 map size:" << n_entries <<  dendl; 
  
  cache_lock.lock();
  eviction_lock.lock();
  del_entry = tail;
  if (del_entry == nullptr) {
    ldout(cct, 10) << "D3nDataCache: lru_eviction: del_entry=null_ptr" << dendl;
	eviction_lock.unlock();
    cache_lock.unlock();
    return 0;
  }

 
  ldout(cct, 10) << __func__  <<" del_id:" << del_entry->obj_id <<dendl;
  lru_remove(del_entry);
  eviction_lock.unlock();

 // cache_lock.lock();
  n_entries = cache_map.size();
  if (n_entries <= 0){
    cache_lock.unlock();
    return -1;
  }
  del_oid = del_entry->obj_id;
  map<string, ChunkDataInfo*>::iterator iter = cache_map.find(del_entry->obj_id);
  if (iter != cache_map.end()) {
	cache_map.erase(del_oid); // oid
  }
  cache_lock.unlock();
  
  int ret = evict_from_directory(del_oid);
  freed_size = del_entry->size;
  free(del_entry);
  //delete del_entry;
  location = cct->_conf->rgw_datacache_path + "/" + del_oid; /*replace tmp with the correct path from config file*/
  remove(location.c_str());
  return freed_size;

}

void DataCache::put_obj(cache_obj* c_obj){
  string obj_id = c_obj->bucket_name +"_"+c_obj->obj_name;
  ldout(cct, 10) << __func__  <<" oid:" << obj_id <<dendl;

  obj_cache_lock.lock();
  map<string, ObjectDataInfo*>::iterator iter = write_cache_map.find(obj_id);
  if (!(iter == write_cache_map.end())){
	struct ObjectDataInfo *chdo = iter->second;
	obj_lru_remove(chdo);
    ldout(cct, 10) << __func__  <<" size_in_byte:"<< chdo->c_obj->size_in_bytes << " " <<obj_id <<dendl;
	chdo->c_obj->creationTime = c_obj->creationTime;
	chdo->c_obj->etag = c_obj->etag;
	chdo->c_obj->size_in_bytes = c_obj->size_in_bytes;
    ldout(cct, 10) << __func__  <<" size_in_byte:"<< chdo->c_obj->size_in_bytes << "after" << c_obj->size_in_bytes << " " <<obj_id <<dendl;
	obj_lru_insert_head(chdo);
  }
  else{
//	ObjectDataInfo *obj_info = NULL;
	cache_obj *nco = new cache_obj();
	nco->bucket_name = c_obj->bucket_name;
	nco->obj_name = c_obj->obj_name;
	nco->size_in_bytes = c_obj->size_in_bytes;
	nco->etag = c_obj->etag;
	nco->owner = c_obj->owner;
	nco->creationTime = c_obj->creationTime;
	ObjectDataInfo *obj_info  = new ObjectDataInfo();
	//obj_info  = new ObjectDataInfo;
	obj_info->obj_id = obj_id;
	obj_info->c_obj = nco;
	obj_info->set_ctx(cct);
	write_cache_map.insert(pair<string, ObjectDataInfo*>(obj_id, obj_info));
	obj_lru_insert_head(obj_info);
  }
  obj_cache_lock.unlock();
}

void DataCache::put(bufferlist& bl, uint64_t len, string oid_orig, cache_block *c_block){

  string obj_id = oid_orig;
  const char x = '/';
  const char y = '_';
  std::replace(obj_id.begin(), obj_id.end(), x, y);
//  string key2 = c_block.c_obj.bucket_name + "_"+tmp_oname+"_"+ std::to_string(c_block.block_id);

  ldout(cct, 10) << __func__  <<" oid:" << obj_id <<dendl;
  int ret = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

  cache_lock.lock(); 
  map<string, ChunkDataInfo *>::iterator iter = cache_map.find(obj_id);
  if (iter != cache_map.end()) {
    cache_lock.unlock();
    ldout(cct, 10) << "Warning: obj data already is cached, no re-write" << dendl;
    return;
  }

  std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),obj_id);
  if (it != outstanding_write_list.end()) {
    cache_lock.unlock();
    ldout(cct, 10) << "Warning: write is already issued, no re-write, obj_id="<< obj_id << dendl;
    return;
  }

  outstanding_write_list.push_back(obj_id);
  cache_lock.unlock();
  
  eviction_lock.lock();
  _free_data_cache_size = free_data_cache_size;
  _outstanding_write_size = outstanding_write_size;
  eviction_lock.unlock();
  
  ldout(cct, 20) << __func__ << "key: "<<obj_id<< "len: "<< len << "free_data_cache_size: "<<free_data_cache_size << "_:"<< free_data_cache_size << "outstanding_write_size  "<< outstanding_write_size << "_ " << _outstanding_write_size << dendl;
  while (len >= (_free_data_cache_size - _outstanding_write_size + freed_size)){
    ldout(cct, 20) << "Datacache Eviction r=" << ret << "key"<<obj_id <<dendl;
    ret = lru_eviction();
    if(ret < 0)
      return;
    freed_size += ret;
  }

  ret = create_aio_write_request(bl, len, obj_id, c_block);
  if (ret < 0) {
    cache_lock.lock();
    outstanding_write_list.remove(obj_id);
    cache_lock.unlock();
    ldout(cct, 10) << "Error: create_aio_write_request is failed"  << ret << dendl;
    return;
  }
  
   eviction_lock.lock();
   free_data_cache_size += freed_size;
   outstanding_write_size += len;
   eviction_lock.unlock();

}

static size_t _remote_req_cb(void *ptr, size_t size, size_t nmemb, void* param) {
  RemoteRequest *req = static_cast<RemoteRequest *>(param);
//  req->bl->append((char *)ptr, size*nmemb);
   req->s.append((char *)ptr, size*nmemb);
  //lsubdout(g_ceph_context, rgw, 1) << __func__ << " data is written "<< " key " << req->key << " size " << size*nmemb  << dendl;
  return size*nmemb;
}

string RemoteS3Request::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "application/x-www-form-urlencoded; charset=utf-8";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;
}


string DataCache::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + "" +"\n" + "" + "\n"+ date + "\n" + CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;
}


string RemoteS3Request::get_date(){
  time_t now = time(0);
  tm *gmtm = gmtime(&now);
  string date;
  char buffer[128];
  std::strftime(buffer,128,"%a, %d %b %Y %X %Z",gmtm);
  date = buffer;
  return date;
}

string DataCache::get_date(){
  time_t now = time(0);
  tm *gmtm = gmtime(&now);
  string date;
  char buffer[128];
  std::strftime(buffer,128,"%a, %d %b %Y %X %Z",gmtm);
  date = buffer;
  return date;
}

string CopyRemoteS3Object::get_date(){
  time_t now = time(0);
  tm *gmtm = gmtime(&now);
  string date;
  char buffer[128];
  std::strftime(buffer,128,"%a, %d %b %Y %X %Z",gmtm);
  date = buffer;
  return date;
}

string CopyRemoteS3Object::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "text/plain";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20);
  return signature;

}

static size_t send_http_data(char *ptr, const size_t size, const size_t nmemb, void * param)
{
 // lsubdout(g_ceph_context, rgw, 1) << __func__ << " data is written "<< " key " << req->bl->length() << dendl; 
  RemoteRequest *req = static_cast<RemoteRequest *>(param);
  size_t ret;
  curl_off_t nread;
  size_t buffer_size = size*nmemb;
  size_t copy_this_much = req->sizeleft;
  
  if(req->sizeleft) {
	size_t copy_this_much = req->sizeleft;
	if(copy_this_much > buffer_size)
      copy_this_much = buffer_size;
	memcpy(ptr, req->readptr, copy_this_much);
    req->readptr += copy_this_much;
	req->sizeleft -= copy_this_much;
	  return copy_this_much;
	  }
	  return 0;

  }

static size_t header_callback(void *pData, size_t tSize, size_t tCount, void *pmUser)
{
  size_t length = tSize * tCount, index = 0;
    while (index < length)
    {
        unsigned char *temp = (unsigned char *)pData + index;
        if ((temp[0] == '\r') || (temp[0] == '\n'))
            break;
        index++;
    }

    std::string str((unsigned char*)pData, (unsigned char*)pData + index);
    std::map<std::string, std::string>* pmHeader = (std::map<std::string, std::string>*)pmUser;
    size_t pos = str.find(' ');
    if (pos != std::string::npos)
        pmHeader->insert(std::pair<std::string, std::string> (str.substr(0, pos), str.substr(pos + 1)));
	
  return tCount;

}




size_t get_callback(void *ptr, size_t size, size_t nmemb, std::string *s)
{
  s->append(static_cast<char *>(ptr), size*nmemb);
  return size*nmemb;
}


std::string ReplaceAll(std::string str, const std::string& from, const std::string& to) {
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
    }
    return str;
}

int DataCache::submit_http_get_requests_s3(cache_obj *c_obj, string prefix, string marker, int max_b){
  CURL *curl_handle;
  std::string s;
  CURLcode res;
  string uri="/"+c_obj->bucket_name+"/";
  string date = get_date();
  string AWSAccessKeyId="TX2XS2M6LVH5WJWBCW53";
  string YourSecretAccessKeyID="BKg6KC5DpUhWDRukuINZidEv06vbTyZQybj2NiIu";
  string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  ldout(cct,10) << __func__ << " bucketname " << c_obj->bucket_name <<" objname  "<< c_obj->obj_name<<  " dest " << dendl; 
  ldout(cct,10) << __func__ << " marker " << marker  <<" prefix  "<< prefix << dendl; 
  uri = "/"+c_obj->bucket_name+"/?delimiter=%2F";

  if (marker != ""){
	string tmp = ReplaceAll(string(marker), std::string("/"), std::string("%2F"));
//	string tmp = ReplaceAll(string(tmp2), std::string("."), std::string("%2C"));
	uri = uri +"&marker=" + tmp;
  }
  if (prefix != ""){
	string tmp = ReplaceAll(string(prefix), std::string("/"), std::string("%2F"));
//	string tmp = ReplaceAll(string(tmp2), std::string("."), std::string("%2C"));
//	ldout(cct,10) << __func__ << " tmp2 "<< tmp2<< " tmp " << tmp <<dendl;
	uri = uri +"&prefix=" + tmp;
  }
//  if (max_b != 0) 
//	uri = uri+"&max-keys="+ to_string(max_b);
  ldout(cct,10) << __func__ << " uri  "<< uri << dendl; 
  
  string loc =  "http://" + cct->_conf->backend_url + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: rgw_datacache";
  curl_handle = curl_easy_init();
   if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
	curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, get_callback);
	curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &s);
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl_handle);
  }
	c_obj->acl = s;
    ldout(cct,10) << __func__ << " url " <<  uri <<"first "<< s << dendl;

	return 0;
}


int DataCache::submit_http_head_requests_s3(cache_obj *c_obj){
  ldout(cct,10) << __func__   << dendl;
  int ret=-1;
  std::map<std::string, std::string> mHeader;
  CURL *curl_handle;
  CURLcode res;
  string uri="/"+c_obj->bucket_name+"/"+c_obj->obj_name;
  string date = get_date();
  string AWSAccessKeyId="TX2XS2M6LVH5WJWBCW53";
  string YourSecretAccessKeyID="BKg6KC5DpUhWDRukuINZidEv06vbTyZQybj2NiIu";
  string signature = sign_s3_request("HEAD", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  "http://" + cct->_conf->backend_url + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
//  string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
  string user_agent="User-Agent: rgw_datacache";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
  //  chunk = curl_slist_append(chunk, content_type.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
	curl_easy_setopt(curl_handle, CURLOPT_HEADERFUNCTION, header_callback);
	curl_easy_setopt(curl_handle, CURLOPT_HEADERDATA, &mHeader);
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
	curl_easy_setopt(curl_handle, CURLOPT_NOBODY, 1L);
//	curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
	curl_easy_setopt(curl_handle, CURLOPT_NOPROGRESS, 1L);
	res = curl_easy_perform(curl_handle); //run the curl command
	curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl_handle);
  }

	std::map<std::string, std::string>::const_iterator itt;
    for (itt = mHeader.begin(); itt != mHeader.end(); itt++)
    {
		ldout(cct,10) << __func__ << "first "<< itt->first << " second " << itt->second << dendl;
		string line = itt->first;
		if (line.find("HTTP") != std::string::npos){
		  string code = itt->second;
		  size_t pos = code.find(' ');
		  if (pos != std::string::npos){
			string strNew = code.substr(0, pos);
			ret = stoull(strNew);
		  }
		}
		if (line.find("Content-Length:") != std::string::npos){
		    string size = itt->second;
		    ldout(cct,10) << __func__ << "Content-Length2: "<< size << dendl;
            c_obj->size_in_bytes = stoull(size);
		  }
		if (line.find("ETag:") != std::string::npos){
		    string etag = itt->second;
		    ldout(cct,10) << __func__ << "ETag: "<< etag << dendl;
            c_obj->etag = etag;
		  }
		if (line.find("Rgwx-Mtime:") != std::string::npos){
		    double lastmod = stod(itt->second);
		    const time_t lastAccessTime = lastmod;
            c_obj->lastAccessTime = lastAccessTime;
			ldout(cct,10) << __func__ << "Rgwx-Mtim: "<< lastmod << " con "<< lastAccessTime << dendl;
		  }
/*		if (line.find("Last-Modified:") != std::string::npos){
		    string lastmod2 = itt->second;
		    time_t lastAccessTime;
			struct tm tm;
			strptime(lastmod2.c_str(), "%a, %d %h %Y %H:%M:%S %Z", &tm);
			time_t t = mktime(&tm);
//            c_obj->lastAccessTime = t;
			ldout(cct,10) << __func__ << "Last-Modified: "<< t<< " con "<< t << dendl;
		  }*/

	  }
	  
 /*Content-Length: 222
< x-amz-request-id: tx00000000000000000002b-00608b7651-48e03f1-default
< Accept-Ranges: bytes
< Content-Type: application/xml
< Date: Fri, 30 Apr 2021 03:15:29 GMT
< Connection: Keep-Alive
*/
		
		
  
  return ret;

}

int CopyRemoteS3Object::submit_http_put_coalesed_requests_s3(){
  
  req->bl = &pbl;
  
  int ret =  store->copy_small_remote(store, owner_obj, t_size, c_obj, out_small_writes, pbl);
  req->sizeleft = t_size;
  req->readptr =  req->bl->c_str();
  //string AWSAccessKeyId = c_obj->accesskey.id;
  //string YourSecretAccessKeyID = c_obj->accesskey.key;
  
  std::time_t result = std::time(0);
  srand(time(NULL));
  float random_num = (float)rand()/RAND_MAX;
  string dest_bucket_name = cct->_conf->coalesced_write_bucket_name;
  const string dest_obj_name = cct->_conf->host +"_"+ std::to_string(result)+std::to_string(store->datacache->getUid());

  CURLcode res;
  string uri="/"+dest_bucket_name+"/"+dest_obj_name;
  string date = get_date();
  string AWSAccessKeyId="TX2XS2M6LVH5WJWBCW53";
  string YourSecretAccessKeyID="BKg6KC5DpUhWDRukuINZidEv06vbTyZQybj2NiIu";
  string signature = sign_s3_request("PUT", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  "http://" + cct->_conf->backend_url + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
//  string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
  string user_agent="User-Agent: rgw_datacache";

   string content_type="Content-Type: text/plain";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
	curl_easy_setopt(curl_handle, CURLOPT_UPLOAD, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_READFUNCTION, send_http_data);
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_READDATA, (void*)req);
	curl_easy_setopt(curl_handle, CURLOPT_INFILESIZE, t_size);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_cleanup(curl_handle);
  }

  if(res == CURLE_HTTP_RETURNED_ERROR) {
	ldout(cct,10) << "__func__ " << " CURLE_HTTP_RETURNED_ERROR" <<curl_easy_strerror(res) << dendl;
	return -1;
	}

  if (res != CURLE_OK)
	return -1;
  
  return 0;




}



int RemoteS3Request::submit_http_get_request_s3(){
  //int begin = req->ofs + req->read_ofs;
  //int end = req->ofs + req->read_ofs + req->read_len - 1;
  off_t begin = req->ofs;
  off_t end = req->ofs + req->read_len - 1;
  std::string range = std::to_string(begin)+ "-"+ std::to_string(end);
  //std::string range = std::to_string( (int)req->ofs + (int)(req->read_ofs))+ "-"+ std::to_string( (int)(req->ofs) + (int)(req->read_ofs) + (int)(req->read_len - 1));
  ldout(cct, 10) << __func__  << " key " << req->key << " range " << range  << " dest "<< req->dest <<dendl;
  
  CURLcode res;
  string uri = "/"+ req->path;;
  //string uri = "/"+req->c_block->c_obj.bucket_name + "/" +req->c_block->c_obj.obj_name;
  string date = get_date();
   
  //string AWSAccessKeyId=req->c_block->c_obj.accesskey.id;
  //string YourSecretAccessKeyID=req->c_block->c_obj.accesskey.key;
  string AWSAccessKeyId=req->ak;
  string YourSecretAccessKeyID=req->sk;
  string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  req->dest + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
  string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
    chunk = curl_slist_append(chunk, "CACHE_GET_REQ:rgw_datacache");
    curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _remote_req_cb);
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
//    curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl_handle);
  }
  if(res == CURLE_HTTP_RETURNED_ERROR) {
   ldout(cct,10) << "__func__ " << " CURLE_HTTP_RETURNED_ERROR" <<curl_easy_strerror(res) << " key " << req->key << dendl;
   return -1;
  } 
  ldout(cct,10) << "__func__ " << " done" << dendl; 
  if (res != CURLE_OK) { return -1;}
  else { return 0; }

}

void RemoteS3Request::run() {

  ldout(cct, 20) << __func__  <<dendl;
  int max_retries = cct->_conf->max_remote_retries;
  int r = 0;
  for (int i=0; i<max_retries; i++ ){
    if(!(r = submit_http_get_request_s3()) && (req->s.size() == req->read_len)){
       ldout(cct, 0) <<  __func__  << "remote get success"<<req->key << " r-id "<< req->r->id << dendl;
//       req->func(req);
        req->finish();
      	return;
    }
    if(req->s.size() != req->read_len){
//#if(req->bl->length() != r->read_len){
       req->s.clear();
    }
    req->s.clear();
    }

    if (r == ECANCELED) {
    ldout(cct, 0) << "ERROR: " << __func__  << "(): remote s3 request for failed, obj="<<req->key << dendl;
    req->r->result = -1;
    req->aio->put(*(req->r));
    return;
    }
  
  
 


}


// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// // vim: ts=8 sw=2 smarttab ft=cpp
#include "rgw_datacache.h"
#include "rgw_perf_counters.h"

#include <iostream>
#include <fstream> // writing to file
#include <sstream> // writing to memory (a string)
#include <errno.h>

#define dout_subsys ceph_subsys_rgw
/*DataCache::DataCache()
  : free_data_cache_size(0) {}
*/
void DataCache::put(bufferlist& bl, uint64_t len, string key, CephContext *cct){
  ldout(cct, 10) << __func__ << key <<dendl;
  int a = 6;
}
/*
DataCache::DataCache()
    : index(0), lock("DataCache"), cache_lock("DataCache::Mutex"), req_lock("DataCache::req"), eviction_lock("DataCache::EvictionMutex"), cct(NULL), io_type(ASYNC_IO), free_data_cache_size(0), outstanding_write_size (0) {}


void DataCache::put(bufferlist& bl, uint64_t len, string key){

  ldout(cct, 10) << __func__ << key <<dendl;
  int ret = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;
  
  cache_lock.lock();
  std::list<std::string>::iterator it = std::find(outstanding_write_list.begin(), outstanding_write_list.end(),key);
    if (it != outstanding_write_list.end()) {
        cache_lock.Unlock();
        ldout(cct, 10) << "data in cache already issued, no rewrite" << dendl;
        return;
    }
    outstanding_write_list.push_back(key);
    cache_lock.unlock();
    
    eviction_lock.lock();
    _free_data_cache_size = free_data_cache_size;
    _outstanding_write_size = outstanding_write_size;
    eviction_lock.unlock();

    ret = create_aio_write_request(bl, len, oid);
    if (ret < 0) {
        cache_lock.lock();
        outstanding_write_list.remove(oid);
        cache_lock.unlock();
        ldout(cct, 1) << " create_aio_wirte_request failed ret "  << ret << dendl;
        return;
    }
    
    eviction_lock.lock();
    free_data_cache_size += freed_size;
    outstanding_write_size += len;
    eviction_lock.unlock(); 
}

void _cache_aio_write_completion_cb(sigval_t sigval) {
    cacheAioWriteRequest *c = (cacheAioWriteRequest *)sigval.sival_ptr;
    c->priv_data->cache_aio_write_completion_cb(c);
}

void DataCache::cache_aio_write_completion_cb(cacheAioWriteRequest *c){

}


int DataCache::create_aio_write_request(bufferlist& bl, uint64_t len, std::string key){
  ldout(cct, 10) << __func__ << key <<dendl;
  struct cacheAioWriteRequest *wr= new struct cacheAioWriteRequest(cct);
  int ret = 0;
    if (wr->create_io(bl, len, key) < 0) {
        ldout(cct, 0) << "ERROR: create_aio_write_request" << dendl;
        goto done;
    }
    wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    wr->cb->aio_sigevent.sigev_notify_function = _cache_aio_write_completion_cb;
    wr->cb->aio_sigevent.sigev_notify_attributes = NULL;
    wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
    wr->oid = oid;
    wr->priv_data = this;

    if((r= ::aio_write(wr->cb)) != 0) {
        ldout(cct, 0) << "ERROR: aio_write "<< r << dendl;
        goto error;
    }
    return 0;
error:
    wr->release();
done:
    return ret;
}

int cacheAioWriteRequest::create_io(bufferlist& bl, uint64_t len, string key) {
    std::string location = "/mnt/a"+key;
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
free_buf:
    cb->aio_buf = NULL;
    free(data);
    data = NULL;

close_file:
    ::close(fd);
done:
    return ret;
}
*/

















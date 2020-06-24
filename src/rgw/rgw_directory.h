
#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"

#include <string>
#include <iostream>
#include <vector>
#include <list>

#include <errno.h>
#include <unistd.h> 
#include <signal.h> 
#include <stdio.h>
#include "include/Context.h"
#include <aio.h>
#include "rgw_threadpool.h"
#include "rgw_cacherequest.h"
#include "rgw_cache.h"
#include <curl/curl.h>

class DataCache;
class CacheThreadPool;
class RemoteS3Request;
//struct cacheAioWriteRequest;
struct RemoteRequest;

using namespace std;

/* the metadata which is written to the directory
 * you can add or remove some of the fields based on
 * your required caching policy
 */
/*
typedef struct objDirectoryStruct {
    string key; //bucketID_ObjectID
    string owner;
    string location;
    uint8_t dirty;
    uint64_t size;
    string createTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
}objectDirectoryStruct_t;

typedef struct blockDirectoryStruct {
    string key; //bucketID_ObjectID_offset
    string owner;
    string location;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
	std::vector<std::pair<std::string, std::string>> popularTenants;
}blockDirectoryStruct_t;

typedef struct cacheStatDirectoryStruct {
    uint64_t hitCount;
    uint64_t reqCount;
    uint64_t capacity;
    string ID;
}cacheStatDirectoryStruct_t;
*/

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
  private:
    int submit_op();
    string sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
    int submit_http_get_request_s3();
    string get_date();
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

  cacheAioWriteRequest(CephContext *_cct) : cct(_cct) , write(false) {}
  int create_io(bufferlist& bl, uint64_t len, std::string key);

  void release() {
    ::close(fd);
    cb->aio_buf = NULL;
    free(data);
    data = NULL;
    free(cb);
    free(this);
  }
};



class DataCache {
  private:
    std::list<string> outstanding_write_list;
    uint64_t capacity ;
    CephContext *cct;
    std::string path;
    CacheThreadPool *tp;
    //ceph::mutex lock = ceph::make_mutex("DataCache::lock");
  public:
    DataCache();
    ~DataCache() {}
    void retrieve_obj_info(cache_obj& c_obj);
    void aging_wb_cache(cache_obj& c_obj, RGWRados *store);
    void submit_remote_req(struct RemoteRequest *c);
    void put(bufferlist& bl, uint64_t len, string key);
    int create_aio_write_request(bufferlist& bl, uint64_t len, std::string key);
    void cache_aio_write_completion_cb(cacheAioWriteRequest *c);
    size_t remove_read_cache_entry(cache_obj& c_obj);
    void init(CephContext *_cct) {
      cct = _cct;
      capacity = 1000;
      path = cct->_conf->rgw_datacache_path;
      tp = new CacheThreadPool(32);
    }
};





class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	virtual int getValue(cache_obj *ptr);
	int setValue(RGWDirectory *dirObj, cache_obj *ptr);
	int updateLastAcessTime(RGWDirectory *dirObj, cache_obj *ptr, string lastAccessTime);
	int updateACL(RGWDirectory *dirObj, cache_obj *ptr, string acl);
	int updateHostList(RGWDirectory *dirObj, cache_obj *ptr, string host);
	int delValue(RGWDirectory *dirObj, cache_obj *ptr);
	//std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);

private:
	virtual int setKey(string key, cache_obj *ptr);
	int delKey(string key);
	int existKey(string key);
	virtual string buildIndex(cache_obj *ptr);

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory() {}
	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}
	int getValue(cache_obj *ptr);
	//std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}
	int getValue(cache_obj *ptr);
	//std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};




#endif

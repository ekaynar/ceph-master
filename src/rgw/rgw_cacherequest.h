#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <aio.h>
#include "rgw_aio.h"
#include "rgw_rest_conn.h"
#include "rgw_rest_client.h"
//#include "rgw_threadpool.h"
struct get_obj_data;

struct AioResult;
class Aio;
class RGWRESTConn;
class RGWRESTStreamRWRequest;
class RGWRadosGetObj;

class CacheRequest {
  public:
    ceph::mutex lock = ceph::make_mutex("CacheRequest");
    struct get_obj_data *op_data;
    int sequence;
    int stat;
    bufferlist *bl;
    uint64_t ofs;
    uint64_t read_ofs;
    uint64_t read_len;   
    rgw::AioResult* r= nullptr;
    std::string key;
    rgw::Aio* aio = nullptr;
    CacheRequest() :  sequence(0), stat(-1), bl(NULL), ofs(0),  read_ofs(0), read_len(0){};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct LocalRequest : public CacheRequest{
  struct aiocb *paiocb;
  LocalRequest() :  CacheRequest(), paiocb(NULL) {}
  ~LocalRequest(){}

  int prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, void (*f)(sigval_t), rgw::Aio* aio, rgw::AioResult* r) {
    this->r = r;	
    this->aio = aio;
    this->bl = bl;
    this->ofs = ofs;
    this->key = key;
    this->read_len = read_len;
    this->stat = EINPROGRESS;	
    std::string loc = "/tmp/" + key;
    struct aiocb *cb = new struct aiocb;
    memset(cb, 0, sizeof(struct aiocb));
    cb->aio_fildes = ::open(loc.c_str(), O_RDONLY);
    if (cb->aio_fildes < 0) {
      return -1;
    }
    cb->aio_buf = malloc(read_len);
    cb->aio_nbytes = read_len;
    cb->aio_offset = read_ofs;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_notify_function = f ;
    cb->aio_sigevent.sigev_notify_attributes = NULL;
    cb->aio_sigevent.sigev_value.sival_ptr = (void*)this;
    this->paiocb = cb;
    return 0;
  }

  void release (){
    lock.lock();
    free((void *)paiocb->aio_buf);
    paiocb->aio_buf = NULL;
    ::close(paiocb->aio_fildes);
    free(paiocb);
    lock.unlock();
    delete this;
  }  

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  int status(){
    lock.lock();
    if (stat != EINPROGRESS) {
      lock.unlock();
      if (stat == ECANCELED){
	release();
	return ECANCELED;
      }}
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    bl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    release();
  }
};

struct RemoteRequest : public CacheRequest{
  string dest;
  int stat;
  void *tp;
  RGWRESTConn *conn;
  RGWRESTStreamRWRequest *in_stream_req;
//  RGWHTTPStreamRWRequest::ReceiveCB *cb;
 // RGWRadosGetObj cb;
  RGWHTTPStreamRWRequest::ReceiveCB *cb{nullptr};
  //RGWRadosGetObj *cb{nullptr};
  //RGWRadosGetObj *cb;
  rgw_obj obj;
  //RemoteRequest(rgw_obj& _obj, RGWRadosGetObj& _cb2) :  CacheRequest() , stat(-1),  obj(_obj), cb2(_cb2){}
  //RemoteRequest(rgw_obj& _obj, class RGWRadosGetObj* _cb) :  CacheRequest() , stat(-1), obj(_obj), cb(_cb){
  RemoteRequest(rgw_obj& _obj, class RGWHTTPStreamRWRequest::ReceiveCB* _cb) :  CacheRequest() , stat(-1), obj(_obj), cb(_cb){
    }
  //RemoteRequest(rgw_obj& _obj, RGWHTTPStreamRWRequest::ReceiveCB *cb) :  CacheRequest() , stat(-1), obj(_obj), cb(cb) {}

  //RemoteRequest() :  CacheRequest() , stat(-1) {}
  ~RemoteRequest(){}
  int prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r) {
    this->r = r;
    this->aio = aio;
    this->bl = bl;
    this->ofs = ofs;
    this->key = key;
    this->read_len = read_len;
    this->dest = dest;
    return 0;
  }
/*
  int submit_op(){
    string etag;
    real_time set_mtime;
    uint64_t expected_size = 0;
    bool prepend_metadata = false;
    bool rgwx_stat = false;
    bool skip_decrypt =true;
    bool get_op = true;
    bool sync_manifest =false;
    bool send = true;
    std::string user = "testuser";
    int ret = this->conn->get_obj(user, ofs, read_len, obj, prepend_metadata,
	                    get_op, rgwx_stat, sync_manifest, skip_decrypt, send);
   // static_cast<RGWRadosGetObj *>(cb), &in_stream_req);
    return 0;

  }
*/

  void release (){
    lock.lock();
    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  void finish(){
    release();
  }
  int status(){
    return 0;
  }

};  

/*
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
   RemoteS3Request(CacheRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
   pthread_mutex_init(&qmtx,0);
   pthread_cond_init(&wcond, 0);
   }
   ~RemoteS3Request() {
   pthread_mutex_destroy(&qmtx);
   pthread_cond_destroy(&wcond);
   }
   virtual void run();
   private:
   int submit_op();
   private:
   pthread_mutex_t qmtx;
   pthread_cond_t wcond;
   CacheRequest *req;
   CephContext *cct;

   };

*/
#endif 

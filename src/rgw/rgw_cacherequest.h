#include "include/rados/librados.hpp"
#include <aio.h>

struct get_obj_data;
class librados::CacheRequest {
  public:
    ceph::mutex lock = ceph::make_mutex("CacheRequest");
    int sequence;
    bufferlist *pbl;
    struct get_obj_data *op_data;
    std::string oid;
    off_t ofs;
    off_t len;
    librados::AioCompletion *lc;
    std::string key;
    off_t read_ofs;
    Context *onack;
    CephContext *cct;
    CacheRequest(CephContext *_cct) : sequence(0), pbl(NULL), ofs(0),  read_ofs(0), cct(_cct) {};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct librados::L1CacheRequest : public librados::CacheRequest{
  int stat;
  struct aiocb *paiocb;
  L1CacheRequest(CephContext *_cct) :  CacheRequest(_cct), stat(-1), paiocb(NULL) {}
  ~L1CacheRequest(){}
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
      }
    }
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    pbl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    onack->complete(0);
    release();
  }
};



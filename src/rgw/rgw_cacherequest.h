#include <aio.h>
#include "rgw_aio.h"
//struct get_obj_data;

struct AioResult;
class Aio;

struct CacheRequest {
  public:
    ceph::mutex lock = ceph::make_mutex("CacheRequest");
    int stat;
    struct aiocb *paiocb;
    int sequence;
    bufferlist *bl;
    int ofs;
    int read_ofs;
    int read_len;   
    rgw::AioResult* r= nullptr;
  //  struct get_obj_data *op_data;
    std::string key;
    rgw::Aio* aio = nullptr;
    CacheRequest() :  stat(-1), paiocb(NULL), sequence(0), bl(NULL), ofs(0),  read_ofs(0), read_len(0){};
    ~CacheRequest(){};

    
    int prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, void (*f)(sigval_t), rgw::Aio* aio, rgw::AioResult* r) {
//	this->sequence = sequence++;
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
	//*cc = c;
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



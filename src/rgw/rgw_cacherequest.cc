#include "rgw_cacherequest.h"
#define COPY_BUF_SIZE (4 * 1024 * 1024)
#include <errno.h>
#define dout_subsys ceph_subsys_rgw



int RemoteRequest::submit_op(){
	
  ldout(cct, 0) << __func__ << "RemoteRequest::submit_op " << ofs<< dendl;
  return 0;
}
int RemoteRequest::prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r){
  this->r = r;
  this->aio = aio;
  this->bl = bl;
  this->ofs = ofs;
  this->key = key;
  this->read_len = read_len;
  this->dest = dest;
  return 0;
}


#include "rgw_cacherequest.h"
#define COPY_BUF_SIZE (4 * 1024 * 1024)
#include <errno.h>
#define dout_subsys ceph_subsys_rgw


int RemoteRequest::prepare_op(std::string key,  bufferlist *bl, off_t read_len, off_t ofs, off_t read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r, cache_block *c_block, string path, void(*func)(RemoteRequest*)){

  this->r = r;
  this->aio = aio;
//  this->bl = bl;
  this->ofs = ofs;
  this->read_ofs = read_ofs;
  this->key = key;
  this->read_len = read_len;
  this->dest = dest;
  this->path = path;
  this->ak = c_block->c_obj.accesskey.id;
  this->sk = c_block->c_obj.accesskey.key;
  this->func= func;
  return 0;
}


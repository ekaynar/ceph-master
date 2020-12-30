// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <type_traits>
#include "include/rados/librados.hpp"
#include "librados/librados_asio.h"

#include "rgw_aio.h"
#include "rgw_cacherequest.h"
#include <aio.h>
//#include "rgw_cache.h"
//Class DataCenter
namespace rgw {
namespace {


struct remote_state {
  Aio* aio;
  RemoteRequest *c;
  remote_state(Aio* aio, AioResult& r)
    : aio(aio) {}
};

void remote_aio_cb(RemoteRequest *c){

/*	 lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Read From Cache " << c->r->id <<" key " << c->key  << dendl;
        auto& r = *(c->r);
        auto s = reinterpret_cast<remote_state*>(&r.user_data); 
  //      c->r->result = 0;
//s->aio->put(*(c->r));
	 lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Read From Cache s " << r.id <<" key " << c->key  << dendl;*/
	c->finish();

}

void cache_aio_cb(sigval_t sigval){
  LocalRequest* c = static_cast<LocalRequest*>(sigval.sival_ptr);
  int status = c->status();
  if (status == ECANCELED) {
	c->r->result = -1;
	c->aio->put(*(c->r));
	return;
  } else if (status == 0) {
	c->finish();
//	delete c;
  }
}

struct cache_state {
  Aio* aio;
  LocalRequest *c;
  cache_state(Aio* aio, AioResult& r)
    : aio(aio) {}

  int submit_op(LocalRequest *cc){
    int ret = 0;
    if((ret = ::aio_read(cc->paiocb)) != 0) {
	  return ret;
	 }
    return ret;
  }
};

void cb(librados::completion_t, void* arg);

struct state {
  Aio* aio;
  librados::AioCompletion* c;

  state(Aio* aio, AioResult& r)
    : aio(aio),
      c(librados::Rados::aio_create_completion(&r, &cb)) {}
};

void cb(librados::completion_t, void* arg) {
  static_assert(sizeof(AioResult::user_data) >= sizeof(state));
  static_assert(std::is_trivially_destructible_v<state>);
  auto& r = *(static_cast<AioResult*>(arg));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = s->c->get_return_value();
  s->c->release();
  s->aio->put(r);
}




template <typename Op>
Aio::OpFunc aio_abstract(Op&& op) {
  return [op = std::move(op)] (Aio* aio, AioResult& r) mutable {
      constexpr bool read = std::is_same_v<std::decay_t<Op>, librados::ObjectReadOperation>;
      auto s = new (&r.user_data) state(aio, r);
      if constexpr (read) {
        r.result = r.obj.aio_operate(s->c, &op, &r.data);
      } else {
        r.result = r.obj.aio_operate(s->c, &op);
      }
      if (r.result < 0) {
        s->c->release();
        aio->put(r);
      }
    };
}

#ifdef HAVE_BOOST_CONTEXT
struct Handler {
  Aio* throttle = nullptr;
  AioResult& r;
  // write callback
  void operator()(boost::system::error_code ec) const {
    r.result = -ec.value();
    throttle->put(r);
  }
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    r.result = -ec.value();
    r.data = std::move(bl);
    throttle->put(r);
  }
};

template <typename Op>
Aio::OpFunc aio_abstract(Op&& op, boost::asio::io_context& context,
                         spawn::yield_context yield) {
  return [op = std::move(op), &context, yield] (Aio* aio, AioResult& r) mutable {
      // arrange for the completion Handler to run on the yield_context's strand
      // executor so it can safely call back into Aio without locking
      using namespace boost::asio;
      async_completion<spawn::yield_context, void()> init(yield);
      auto ex = get_associated_executor(init.completion_handler);

      auto& ref = r.obj.get_ref();
      librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, 0,
                              bind_executor(ex, Handler{aio, r}));
    };
}

template <typename Op>
Aio::OpFunc cache_aio_abstract(Op&& op, off_t obj_ofs, off_t read_ofs, off_t read_len, string location, boost::asio::io_context& context, spawn::yield_context yield) {
  return [op = std::move(op), obj_ofs, read_ofs, read_len, location, &context, yield] (Aio* aio, AioResult& r) mutable{

  using namespace boost::asio;
  async_completion<spawn::yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler); 
  auto& ref = r.obj.get_ref();
  //auto cs = new (&r.user_data) cache_state(aio, r);
  LocalRequest* c = new LocalRequest();
  c->prepare_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, cache_aio_cb, aio, &r, location);
  librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, c,
                              bind_executor(ex, Handler{aio, r}));
  int ret = c->submit_op();
  if ( ret < 0 ) {
      r.result = -1;
      aio->put(r);
  }
  };
}

template <typename Op>
Aio::OpFunc remote_aio_abstract(Op&& op, off_t obj_ofs, off_t read_ofs, off_t read_len, string dest,  RemoteRequest *c, boost::asio::io_context& context, spawn::yield_context yield, cache_block *c_block, string path, DataCache *dc) {
  return [op = std::move(op), obj_ofs, read_ofs, read_len, dest, c, &context, yield, c_block, path, dc] (Aio* aio, AioResult& r) mutable{

      /*	  auto& ref = r.obj.get_ref();
    auto cs = new(&r.user_data) remote_state(aio, r);
    cs->c = new RemoteRequest();
    cs->c->prepare_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, dest, aio, &r, c_block, path, remote_aio_cb);
    dc->submit_remote_req(cs->c);

*/
	
  using namespace boost::asio;
  async_completion<spawn::yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler);
  auto& ref = r.obj.get_ref();
  RemoteRequest* cc = new RemoteRequest();
  cc->prepare_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, dest, aio, &r, c_block, path, remote_aio_cb);
  librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, cc,
                             bind_executor(ex, Handler{aio, r}));
   //DataCache *ab = static_cast<DataCache *>(dc);
   dc->submit_remote_req(cc);

  };
}


/* datacache */

#endif // HAVE_BOOST_CONTEXT
template <typename Op>
Aio::OpFunc cache_aio_abstract(Op&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, off_t read_len, string location) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);

  #ifdef HAVE_BOOST_CONTEXT
  return cache_aio_abstract(std::forward<Op>(op),obj_ofs, read_ofs, read_len, location, y.get_io_context(), y.get_yield_context());
  #endif
}

template <typename Op>
Aio::OpFunc remote_aio_abstract(Op&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, off_t read_len, string dest,  RemoteRequest *c, cache_block *c_block, string path, DataCache *dc) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
  #ifdef HAVE_BOOST_CONTEXT
  return remote_aio_abstract(std::forward<Op>(op),obj_ofs, read_ofs, read_len, dest, c,  y.get_io_context(), y.get_yield_context(),c_block , path, dc);
  #endif
}
/* datacache */
template <typename Op>
Aio::OpFunc aio_abstract(Op&& op, optional_yield y) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    return aio_abstract(std::forward<Op>(op), y.get_io_context(),
                        y.get_yield_context());
  }
#endif
  return aio_abstract(std::forward<Op>(op));
}

} // anonymous namespace

Aio::OpFunc Aio::librados_op(librados::ObjectReadOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}
Aio::OpFunc Aio::librados_op(librados::ObjectWriteOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}

/* datacache */
Aio::OpFunc Aio::cache_op(librados::ObjectReadOperation&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, off_t read_len, string location) {
    return cache_aio_abstract(std::move(op), y, obj_ofs, read_ofs, read_len, location);
}

Aio::OpFunc Aio::remote_op(librados::ObjectReadOperation&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, off_t read_len, string dest,  RemoteRequest *c, cache_block *c_block, string path, DataCache *dc) {
    return remote_aio_abstract(std::move(op), y, obj_ofs, read_ofs, read_len, dest, c, c_block, path, dc);
}

} // namespace rgw

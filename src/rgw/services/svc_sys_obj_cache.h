// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"
#include "rgw/rgw_cache.h"
#include "rgw/rgw_directory.h"

#include "svc_sys_obj_core.h"

class RGWSI_Notify;

class RGWSI_SysObj_Cache_CB;
class RGWSI_SysObj_Cache_ASocketHook;

class RGWSI_SysObj_Cache : public RGWSI_SysObj_Core
{
  friend class RGWSI_SysObj_Cache_CB;
  friend class RGWServices_Def;
  friend class ASocketHandler;

  RGWSI_Notify *notify_svc{nullptr};
  ObjectCache cache;
  DataCache datacache; //datacache

  std::shared_ptr<RGWSI_SysObj_Cache_CB> cb;

  void normalize_pool_and_obj(const rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj);
protected:
  void init(RGWSI_RADOS *_rados_svc,
            RGWSI_Zone *_zone_svc,
            RGWSI_Notify *_notify_svc) {
    core_init(_rados_svc, _zone_svc);
    notify_svc = _notify_svc;
  }

  int do_start() override;
  void shutdown() override;

  int raw_stat(const rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
               map<string, bufferlist> *attrs, bufferlist *first_chunk,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y) override;

  int read(RGWSysObjectCtxBase& obj_ctx,
           RGWSI_SysObj_Obj_GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           const rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           map<string, bufferlist> *attrs,
	   bool raw_attrs,
           rgw_cache_entry_info *cache_info,
           boost::optional<obj_version>,
           optional_yield y) override;

  int get_attr(const rgw_raw_obj& obj, const char *name, bufferlist *dest,
               optional_yield y) override;

  int set_attrs(const rgw_raw_obj& obj, 
                map<string, bufferlist>& attrs,
                map<string, bufferlist> *rmattrs,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);

  int remove(RGWSysObjectCtxBase& obj_ctx,
             RGWObjVersionTracker *objv_tracker,
             const rgw_raw_obj& obj,
             optional_yield y) override;

  int write(const rgw_raw_obj& obj,
            real_time *pmtime,
            map<std::string, bufferlist>& attrs,
            bool exclusive,
            const bufferlist& data,
            RGWObjVersionTracker *objv_tracker,
            real_time set_mtime,
            optional_yield y) override;

  int write_data(const rgw_raw_obj& obj,
                 const bufferlist& bl,
                 bool exclusive,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);

  int distribute_cache(const string& normal_name, const rgw_raw_obj& obj,
                       ObjectCacheInfo& obj_info, int op,
                       optional_yield y);

  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);

  void set_enabled(bool status);

public:
  RGWSI_SysObj_Cache(CephContext *cct) : RGWSI_SysObj_Core(cct), asocket(this) {
    cache.set_ctx(cct);
    datacache.init(cct); //datacache
  }

  DataCache get_datacache() {
    return datacache;
  }
  bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                         RGWChainedCache::Entry *chained_entry);
  void register_chained_cache(RGWChainedCache *cc);
  void unregister_chained_cache(RGWChainedCache *cc);

  class ASocketHandler {
    RGWSI_SysObj_Cache *svc;

    std::unique_ptr<RGWSI_SysObj_Cache_ASocketHook> hook;

  public:
    ASocketHandler(RGWSI_SysObj_Cache *_svc);
    ~ASocketHandler();

    int start();
    void shutdown();

    // `call_list` must iterate over all cache entries and call
    // `cache_list_dump_helper` with the supplied Formatter on any that
    // include `filter` as a substring.
    //
    void call_list(const std::optional<std::string>& filter, Formatter* f);

    // `call_inspect` must look up the requested target and, if found,
    // dump it to the supplied Formatter and return true. If not found,
    // it must return false.
    //
    int call_inspect(const std::string& target, Formatter* f);

    // `call_erase` must erase the requested target and return true. If
    // the requested target does not exist, it should return false.
    int call_erase(const std::string& target);

    // `call_zap` must erase the cache.
    int call_zap();
  } asocket;
};

template <class T>
class RGWChainedCacheImpl : public RGWChainedCache {
  RGWSI_SysObj_Cache *svc{nullptr};
  ceph::timespan expiry;
  RWLock lock;

  std::unordered_map<std::string, std::pair<T, ceph::coarse_mono_time>> entries;

public:
  RGWChainedCacheImpl() : lock("RGWChainedCacheImpl::lock") {}
  ~RGWChainedCacheImpl() {
    if (!svc) {
      return;
    }
    svc->unregister_chained_cache(this);
  }

  void unregistered() override {
    svc = nullptr;
  }

  void init(RGWSI_SysObj_Cache *_svc) {
    if (!_svc) {
      return;
    }
    svc = _svc;
    svc->register_chained_cache(this);
    expiry = std::chrono::seconds(svc->ctx()->_conf.get_val<uint64_t>(
				    "rgw_cache_expiry_interval"));
  }

  boost::optional<T> find(const string& key) {
    std::shared_lock rl{lock};
    auto iter = entries.find(key);
    if (iter == entries.end()) {
      return boost::none;
    }
    if (expiry.count() &&
	(ceph::coarse_mono_clock::now() - iter->second.second) > expiry) {
      return boost::none;
    }

    return iter->second.first;
  }

  bool put(RGWSI_SysObj_Cache *svc, const string& key, T *entry,
	   std::initializer_list<rgw_cache_entry_info *> cache_info_entries) {
    if (!svc) {
      return false;
    }

    Entry chain_entry(this, key, entry);

    /* we need the svc cache to call us under its lock to maintain lock ordering */
    return svc->chain_cache_entry(cache_info_entries, &chain_entry);
  }

  void chain_cb(const string& key, void *data) override {
    T *entry = static_cast<T *>(data);
    std::unique_lock wl{lock};
    entries[key].first = *entry;
    if (expiry.count() > 0) {
      entries[key].second = ceph::coarse_mono_clock::now();
    }
  }

  void invalidate(const string& key) override {
    std::unique_lock wl{lock};
    entries.erase(key);
  }

  void invalidate_all() override {
    std::unique_lock wl{lock};
    entries.clear();
  }
}; /* RGWChainedCacheImpl */

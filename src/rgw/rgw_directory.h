
#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>
#include <vector>
#include <list>

using namespace std;

class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	int existKey(string key);
	int delKey(string key);
	CephContext *cct;

private:
	//virtual int setKey(string key, cache_obj *ptr);

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory() {}
	cpp_redis::client client;
	void init(CephContext *_cct) {
      		cct = _cct;
		client.connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port);
    	}

	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}
	int setValue(cache_obj *ptr);
	int getValue(cache_obj *ptr);
	int updateHostsList(cache_obj *ptr);
	int updateHomeLocation(cache_obj *ptr);
	int updateACL(cache_obj *ptr);
	int updateLastAcessTime(cache_obj *ptr);
	int delValue(cache_obj *ptr);
	vector<pair<vector<string>, time_t>> get_aged_keys(time_t startTime, time_t endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	 cpp_redis::client client;
	void init(CephContext *_cct) {
		cct = _cct;
		client.connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port);
	}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}
	int setValue(cache_block *ptr);
	int getValue(cache_block *ptr);
	int updateHostsList(cache_block *ptr);
	int delValue(cache_block *ptr);

private:
	int setKey(string key, cache_block *ptr);
	string buildIndex(string bucket_name, string obj_name, uint64_t block_id);
	
};




#endif


#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
#include <sw/redis++/redis++.h>
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
	CephContext *cct;


private:
	//virtual int setKey(string key, cache_obj *ptr);

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory() {}
	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}
	cpp_redis::client client;
	void init(CephContext *_cct) {
      		cct = _cct;
		client.connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port);
    	}

	int existKey(string key);
	int delKey(string key);
	int setValue(cache_obj *ptr);
	int getValue(cache_obj *ptr);
	int updateField(cache_obj *ptr, string field);
	//int updateHostsList(cache_obj *ptr);
	//int updateHomeLocation(cache_obj *ptr);
	//int updateACL(cache_obj *ptr);
	//int updateLastAcessTime(cache_obj *ptr);
	int delValue(cache_obj *ptr);
	vector<pair<vector<string>, time_t>> get_aged_keys(time_t startTime, time_t endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	virtual ~RGWBlockDirectory() { cout << "RGWBlock Directory is destroyed!";}
	cpp_redis::client client;
	void init(CephContext *_cct) {
		cct = _cct;
		client.connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port);
	}
	
	int existKey(string key);
	int delKey(string key);
	int setValue(cache_block *ptr);
	int getValue(cache_block *ptr);
	int updateField(cache_block *ptr, string field);
	//int updateHostsList(cache_block *ptr);
	int delValue(cache_block *ptr);

private:
	int setKey(string key, cache_block *ptr);
	string buildIndex(string bucket_name, string obj_name, uint64_t block_id);
	
};




#endif

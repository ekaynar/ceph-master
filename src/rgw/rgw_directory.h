
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
#include <cstdint>

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
	cpp_redis::client *client;
	cpp_redis::client client1;
	cpp_redis::client client2;
	cpp_redis::client client3;
	void init(CephContext *_cct) {
      		cct = _cct;
		client1.connect(cct->_conf->rgw_directory_address1, cct->_conf->rgw_directory_port1);
		client2.connect(cct->_conf->rgw_directory_address2, cct->_conf->rgw_directory_port2);
		client3.connect(cct->_conf->rgw_directory_address3, cct->_conf->rgw_directory_port3);
    	}

	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}
    void findClient(string key);
	int delKey(string key);
	int existKey(string key);
	int setValue(cache_obj *ptr);
	int getValue(cache_obj *ptr);
	//int updateHostsList(cache_obj *ptr);
	//int updateHomeLocation(cache_obj *ptr);
	//int updateACL(cache_obj *ptr);
	//int updateLastAcessTime(cache_obj *ptr);
	int updateField(cache_obj *ptr, string field, string value);
	int delValue(cache_obj *ptr);
	vector<string> get_aged_keys(time_t startTime, time_t endTime, int NKey);

private:
	//int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	 cpp_redis::client *client;
	 cpp_redis::client client1;
	 cpp_redis::client client2;
	 cpp_redis::client client3;
	void init(CephContext *_cct) {
		cct = _cct;
		client1.connect(cct->_conf->rgw_directory_address1, cct->_conf->rgw_directory_port1);
		client2.connect(cct->_conf->rgw_directory_address2, cct->_conf->rgw_directory_port2);
		client3.connect(cct->_conf->rgw_directory_address3, cct->_conf->rgw_directory_port3);
	}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}
    void findClient(string key);
	int delKey(string key);
	int existKey(string key);
	int setValue(cache_block *ptr);
	int getValue(cache_block *ptr);
	//int updateHostsList(cache_block *ptr);
	int updateField(cache_block *ptr, string field, string value);
	int delValue(cache_block *ptr);

private:
	//int setKey(string key, cache_block *ptr);
	string buildIndex(cache_block *ptr);
	
};




#endif


#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
//#include <sw/redis++/redis++.h>
//#include <cpp_redis/cpp_redis>
#include <hiredis-vip/hircluster.h>
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
    redisClusterContext *cc;
	void init(CephContext *_cct) {
      	cct = _cct;
		cc = redisClusterContextInit();

        //redisClusterSetOptionAddNodes(cc, "192.168.0.92:7000,192.168.0.92:7001,192.168.0.92:7002");
        redisClusterSetOptionAddNodes(cc, "192.168.0.92:7000");
        redisClusterSetOptionRouteUseSlots(cc);
        redisClusterConnect2(cc);
        if(cc == NULL || cc->err)
            return;
    }

	int existKey(string key);

	int setKey(cache_obj *ptr);
	int getKey(cache_obj *ptr);
	int updateField(cache_obj *ptr, string field, string value);
	int delKey(cache_obj *ptr);
//	vector<pair<vector<string>, time_t>> get_aged_keys(time_t startTime, time_t endTime);

private:
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	virtual ~RGWBlockDirectory() { cout << "RGWBlock Directory is destroyed!";}
    redisClusterContext *cc;
	void init(CephContext *_cct) {
		cct = _cct;
    	cc = redisClusterContextInit();

        //redisClusterSetOptionAddNodes(cc, "192.168.0.92:7000,192.168.0.92:7001,192.168.0.92:7002");
        redisClusterSetOptionAddNodes(cc, "192.168.0.92:7000");
        redisClusterSetOptionRouteUseSlots(cc);
        redisClusterConnect2(cc);
        if(cc == NULL || cc->err)
            return;

	}
	
	int existKey(string key);
	int setKey(cache_block *ptr);
	int getKey(cache_block *ptr);
	int updateField(cache_block *ptr, string field, string value);
	int delKey(cache_block *ptr);

private:
	string buildIndex(cache_block *ptr);
	
};




#endif

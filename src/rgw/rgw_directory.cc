#include <errno.h>
//#include <cpp_redis/cpp_redis>
//#include <sw/redis++/redis++.h>
#include "rgw_directory.h"
#include <hiredis-vip/hircluster.h>
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>
#define dout_subsys ceph_subsys_rgw


inline const string BoolToString(bool b)
{	
	return b ? "true" : "false";
}

inline const bool StringToBool(string s)
{
	if (s == "true")
		return true;
	else 
		return false;	
}

string locationToString( int enumVal )
{
	switch(enumVal)
	{
		case CacheLocation::LOCAL_READ_CACHE:
			return "readCache";
		case CacheLocation::WRITE_BACK_CACHE:
			return "writeCache";
		case CacheLocation::REMOTE_CACHE:
			return "remoteCache" ;
		case CacheLocation::DATALAKE:
			return "dataLake";

		default:
			return "Not recognized..";
	}
}

string homeToString( int enumVal )
{
	switch(enumVal)
	{
		case HomeLocation::CACHE:
			return "cache";
		case HomeLocation::BACKEND:
			return "backend";

		default:
			return "Not recognized..";
	}
}

HomeLocation stringToHome(string home)
{
	if (home == "cache")
		return HomeLocation::CACHE;
	else if (home == "backend")
		return HomeLocation::BACKEND;
}



string protocolToString( int enumVal )
{
	switch(enumVal)
	{
		case BackendProtocol::S3:
			return "s3";
		case BackendProtocol::LIBRADOS:
			return "librados";
		case BackendProtocol::SWIFT:
			return "swift" ;

		default:
			return "Not recognized..";
	}
}



BackendProtocol stringToProtocol(string protocol)
{
	if (protocol == "s3")
		return BackendProtocol::S3;
	else if (protocol == "librados")
		return BackendProtocol::LIBRADOS;
	else if (protocol == "swift")
		return BackendProtocol::SWIFT;
	else
		return BackendProtocol::S3;
}

time_t stringToTime(string time)
{
	struct tm tm;
	strptime(time.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
	time_t t = mktime(&tm); 
	return t;
}

string timeToString(time_t time)
{
	struct tm *tm = new struct tm;
	tm = gmtime(&time);

	// format: %Y-%m-%d %H:%M:%S
	string time_s = to_string(tm->tm_year + 1900)+"-"+to_string(tm->tm_mon + 1)+"-"+to_string(tm->tm_mday)+" "+to_string(tm->tm_hour)+":"+to_string(tm->tm_min)+":"+to_string(tm->tm_sec);
	return time_s;
}


int RGWObjectDirectory::existKey(string key){
    int result = 0;
    redisReply *reply = (redisReply *)redisClusterCommand(cc, "exists %s", key);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object Key exists function: " << cc->errstr <<dendl;
        return -1;
    }
    
    result = reply->integer;
    freeReplyObject(reply);
    return result;

}


int RGWBlockDirectory::existKey(string key){
    int result = 0;
    redisReply *reply = (redisReply *)redisClusterCommand(cc, "exists %s", key);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Block Key exists function: " << cc->errstr <<dendl;
        return -1;
    }
    
    result = reply->integer;
    freeReplyObject(reply);
    return result;
}


int RGWObjectDirectory::updateField(cache_obj *ptr, string field, string value){

	string key = buildIndex(ptr);

	if (!existKey(key))
		return -1;

    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmset %s %s %s", key, field, value);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object UPDATE: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
	
}

int RGWBlockDirectory::updateField(cache_block *ptr, string field, string value){

	string key = buildIndex(ptr);

	if (!existKey(key))
		return -1;

    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmset %s %s %s", key, field, value);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Block UPDATE: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
}

int RGWObjectDirectory::delKey(cache_obj *ptr){
    string key = buildIndex(ptr);
    redisReply *reply = (redisReply *)redisClusterCommand(cc, "del %s", key);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object Delete key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
}

int RGWBlockDirectory::delKey(cache_block *ptr){
    string key = buildIndex(ptr);
    redisReply *reply = (redisReply *)redisClusterCommand(cc, "del %s", key);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Block Delete key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
}

string RGWObjectDirectory::buildIndex(cache_obj *ptr){
    std::stringstream ss;
    ss << ptr->lastAccessTime;
    std::string ts = ss.str();

	return ptr->bucket_name + "_" + ptr->obj_name + ":" + ts;
}

string RGWBlockDirectory::buildIndex(cache_block *ptr){
    std::stringstream ss;
    ss << ptr->lastAccessTime;
    std::string ts = ss.str();

	return ptr->c_obj.bucket_name + "_" + ptr->c_obj.obj_name + "_" + to_string(ptr->block_id) + ":" + ts;
}


int RGWObjectDirectory::setKey(cache_obj *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
	string key = buildIndex(ptr);

	string hosts;
	stringstream ss;
	for(size_t i = 0; i < ptr->hosts_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->hosts_list[i];
	}
	hosts = ss.str();

    string fields;
	//creating a list of key's properties
	fields = " key " +  key +" owner " + ptr->owner + " obj_acl " +  ptr->acl + 
	            " aclTimeStamp " + timeToString(ptr->aclTimeStamp) + " hosts " + hosts +
	            " dirty " + BoolToString(ptr->dirty) + " size " + to_string(ptr->size_in_bytes) +
            	" creationTime " + timeToString(ptr->creationTime) + " lastAccessTime " + timeToString(ptr->lastAccessTime) +
            	" etag " + ptr->etag + " backendProtocol " + protocolToString(ptr->backendProtocol) +
	            " bucket_name " + ptr->bucket_name + " obj_name " + ptr->obj_name + 
            	" home_location " + homeToString(ptr->home_location);




    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmset %s %s", key, fields);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object set Key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
	
}

int RGWBlockDirectory::setKey(cache_block *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
	string key = buildIndex(ptr);

	string hosts;
	stringstream ss;
	for(size_t i = 0; i < ptr->hosts_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->hosts_list[i];
	}
	hosts = ss.str();

    string fields;
	//creating a list of key's properties
	fields = " key " +  key +" owner " + ptr->c_obj.owner + " hosts " + hosts +
	            " size " + to_string(ptr->size_in_bytes) + " bucket_name " + ptr->c_obj.bucket_name +
                " obj_name " + ptr->c_obj.obj_name + " block_id " +  to_string(ptr->block_id) + 
            	" lastAccessTime " + timeToString(ptr->lastAccessTime) + " accessCount " + to_string(ptr->freq);

    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmset %s %s", key, fields);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Block set Key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	return 1;
	
}
 

int RGWObjectDirectory::getKey(cache_obj *ptr){
    string key = buildIndex(ptr);
    string owner;
    string obj_acl;
    string aclTimeStamp;
    string hosts;
    string dirty;
    string size;
    string creationTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
    string home_location;

    //ldout(cct,10) << __func__ << "getKey" << dendl;
	
    string fields = string("key ") + " owner " + " obj_acl " + " aclTimeStamp " + " hosts " +
	            " dirty " + " size " + " creationTime " + " lastAccessTime " +
            	" etag " + " backendProtocol " + " bucket_name " + " obj_name " + 
            	" home_location ";

    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmget %s %s", key, fields);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object get Key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    if (reply->type == REDIS_REPLY_ARRAY) {
        key = reply->element[0]->str; 
        owner = reply->element[1]->str; 
        obj_acl = reply->element[2]->str; 
        aclTimeStamp = reply->element[3]->str; 
        hosts = reply->element[4]->str; 
        dirty = reply->element[5]->str; 
        size = reply->element[6]->str; 
        creationTime = reply->element[7]->str; 
        lastAccessTime = reply->element[8]->str; 
        etag = reply->element[9]->str; 
        backendProtocol = reply->element[10]->str; 
        bucket_name = reply->element[11]->str; 
        obj_name = reply->element[12]->str; 
        home_location = reply->element[13]->str;
    }

    freeReplyObject(reply);

	stringstream sloction(hosts);
	string tmp;

	//passing the values to the requester
	ptr->owner = owner;
	ptr->acl = obj_acl;
	ptr->aclTimeStamp = stringToTime(aclTimeStamp);

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
		ptr->hosts_list.push_back(tmp);

	ptr->dirty = StringToBool(dirty);
	ptr->size_in_bytes = stoull(size);
	ptr->creationTime = stringToTime(creationTime);
	ptr->lastAccessTime = stringToTime(lastAccessTime);
	ptr->etag = etag;
	ptr->backendProtocol = stringToProtocol(backendProtocol);
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;
	ptr->home_location = stringToHome(home_location);
//	ldout(cct,10) <<"after connect" << size << bucket_name << obj_name<< owner<<etag <<dendl;
	return 1;
}


int RGWBlockDirectory::getKey(cache_block *ptr){

    string key = buildIndex(ptr);
    //ldout(cct,10) << __func__ << " key " << key<< " obj name "<< ptr->c_obj.obj_name <<dendl;

    string owner;
    string hosts;
    string size;
    string etag;
    string bucket_name;
    string obj_name;
    string block_id;
    string freq;

    //ldout(cct,10) << __func__ << "getKey" << dendl;
	
    string fields = string("key ") + " owner " + " hosts " +
	            " size " + " bucket_name " + " obj_name " + 
            	" block_id " + " accessCount ";

    redisReply *reply = (redisReply *)redisClusterCommand(cc, "hmget %s %s", key, fields);
    if(reply == NULL)
    {
	    ldout(cct,10) <<"REDIS Object get Key: error is: " << cc->errstr <<dendl;
        return -1;
    }

    if (reply->type == REDIS_REPLY_ARRAY) {
        key = reply->element[0]->str; 
        owner = reply->element[1]->str; 
        hosts = reply->element[2]->str; 
        size = reply->element[3]->str; 
        bucket_name = reply->element[4]->str; 
        obj_name = reply->element[5]->str; 
        block_id = reply->element[6]->str; 
        freq = reply->element[7]->str;
    }
    else
    {
	    ldout(cct,10) <<"REDIS Block get Key: field is not array " <<dendl;
        return -1;
    }

    freeReplyObject(reply);

	stringstream sloction(hosts);
	string tmp;

	//passing the values to the requester
	ptr->c_obj.owner = owner;

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
		ptr->hosts_list.push_back(tmp);

	ptr->size_in_bytes = stoull(size);
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	ptr->block_id = stoull(block_id);
	
	ptr->freq = stoull(freq);

	return 1;
}


vector<pair<vector<string>, time_t>> RGWObjectDirectory::get_aged_keys(time_t searchTime){
	vector<pair<string, string>> list;
	vector<pair<vector<string>, time_t>> keys; //return aged keys
	string key;
	string time;
	int rep_size = 0;

    std::stringstream ss;
    ss << ptr->lastAccessTime;
    std::string searchTime = ss.str();

	std::string dirKey = "keyObjectDirectory";
	client.zrangebyscore(dirKey, startTime, endTime, true, [&key, &time, &list](cpp_redis::reply &reply){
	      auto arr = reply.as_array();
	      for (unsigned i = 0; i < reply.as_array().size(); i+=2)
	      {
	          key = arr[i].as_string();
	          time = arr[i+1].as_string();

			  list.push_back(make_pair(key, time));
	      }
	      //if (reply.is_error() == false)
	});
	client.sync_commit();

	rep_size = list.size();
	for (int i=0; i < rep_size; i++){
		vector<string> vTmp;
		string owner;

	    stringstream key(list[i].first);
		string sTmp;
		//bucket_object_chunkID
    	while(getline(key, sTmp, '_'))
        	vTmp.push_back(sTmp);

		//getting owner for the directory
		client.hget(list[i].first, "owner", [&owner](cpp_redis::reply &reply){
		  auto arr = reply.as_array();
          owner = arr[0].as_string();
    	});
		client.sync_commit();

		vTmp.push_back(owner);

		//creationTime and return value
		keys.push_back(make_pair(vTmp, stringToTime(list[i].second)));
				
	}
	

	return keys;
}

*/


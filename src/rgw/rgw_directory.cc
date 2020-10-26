

#include <errno.h>
#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
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

/* this function should be implemented in their own respected classes */
/*
int RGWDirectory::getValue(cache_obj *ptr){
	cout << "wrong function!";
	return -1;
}


int RGWDirectory::setKey(string key, cache_obj *ptr){
	cout << "wrong function!";
	return -1;
}

*/

int RGWDirectory::existKey(string key){
	cpp_redis::client client;
	client.connect("192.168.32.103", 7000);

	int result = 0;

    vector<string> keys;
    keys.push_back(key);

	client.exists(keys, [&result](cpp_redis::reply &reply){
		auto arr = reply.as_array();
		result = arr[0].as_integer();
	});
	return result;
}

/* updatinh the directory value of host_list
 * its input is a host which has a copy of the data
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWObjectDirectory::updateHostsList(cache_obj *ptr){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;

	string key = buildIndex(&tmpObj);

		//getting old values from the directory
	if (getValue(&tmpObj) == 0){

		//updating the desired field
		tmpObj.hosts_list = ptr->hosts_list;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

/* updatinh the directory value of host_list
 * its input is a host which has a copy of the data
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWObjectDirectory::updateHomeLocation(cache_obj *ptr){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;

	string key = buildIndex(&tmpObj);

	//getting old values from the directory
	if (getValue(&tmpObj) == 0){

		//updating the desired field
		tmpObj.home_location = ptr->home_location;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}



int RGWBlockDirectory::updateHostsList(cache_block *ptr){

	cache_block tmpObj;
	
	//we need to build the key to find the object in the directory
	string bucket_name = ptr->c_obj.bucket_name;
	string obj_name = ptr->c_obj.obj_name;
	uint64_t block_id = ptr->block_id;

	string key = buildIndex(bucket_name, obj_name, block_id);

	//getting old values from the directory
	if (getValue(&tmpObj) == 0){

		//updating the desired field
		tmpObj.hosts_list = ptr->hosts_list;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}



/* updatinh the directory value of acl_obj
 * its input is a new acl of the object
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWObjectDirectory::updateACL(cache_obj *ptr){
	string acl = ptr->acl;
	time_t aclTimeStamp = ptr->aclTimeStamp;

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;

	string key = buildIndex(&tmpObj);

	//getting old values from the directory
	if (getValue(&tmpObj) == 0){

		tmpObj.acl = acl;
		tmpObj.aclTimeStamp = aclTimeStamp;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

/* updatinh the directory value of lastAccessTime
 * its input is object's ceph::real_time last access time 
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWObjectDirectory::updateLastAcessTime(cache_obj *ptr){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;

	string key = buildIndex(&tmpObj);

	//getting old values from the directory
	if (getValue(&tmpObj) == 0){

		tmpObj.lastAccessTime = ptr->lastAccessTime;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

int RGWObjectDirectory::delValue(cache_obj *ptr){
    string key = buildIndex(ptr);
	int result = 0;

	result += RGWDirectory::delKey(key);
	return result;
}

int RGWBlockDirectory::delValue(cache_block *ptr){
    string key = buildIndex(ptr->c_obj.bucket_name, ptr->c_obj.obj_name, ptr->block_id);
	int result = 0;

	result += RGWDirectory::delKey(key);
	return result;
}

int RGWDirectory::delKey(string key){
	int result = 0;
    vector<string> keys;
    keys.push_back(key);

	cpp_redis::client client;
	client.connect("192.168.32.103", 7000);

	client.del(keys, [&result](cpp_redis::reply &reply){
		auto arr = reply.as_array();
		result = arr[0].as_integer();
	});
	return result;
}

/* builds the index for the directory
 * based on bucket_name, obj_name, and chunk_id
 */
string RGWObjectDirectory::buildIndex(cache_obj *ptr){
	return ptr->bucket_name + "_" + ptr->obj_name;
}

/* builds the index for the directory
 * based on bucket_name, obj_name, chunk_id, and etag
 */
string RGWBlockDirectory::buildIndex(string bucket_name, string obj_name, uint64_t block_id){
	return bucket_name + "_" + obj_name + "_" + to_string(block_id);
}


/* adding a key to the directory
 * if the key exists, it will be deleted and then the new value be added
 */
int RGWObjectDirectory::setValue(cache_obj *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
	string key = buildIndex(ptr);

	//delete the existing key, 
	//to update an existing key, updateValue() should be used
	if (RGWDirectory::existKey(key))
		RGWDirectory::delKey(key);

	return setKey(key, ptr);
	
}

int RGWBlockDirectory::setValue(cache_block *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
    string key = buildIndex(ptr->c_obj.bucket_name, ptr->c_obj.obj_name, ptr->block_id);

	//delete the existing key, 
	//to update an existing key, updateValue() should be used
	if (RGWDirectory::existKey(key))
		RGWDirectory::delKey(key);

	return setKey(key, ptr);
	
}
 
/* the horse function to add a new key to the directory
 */
int RGWObjectDirectory::setKey(string key, cache_obj *ptr){
//	cpp_redis::client client;
//	client.connect("192.168.32.103", 7000);

	vector<pair<string, string>> list;
	vector<string> keys;
	multimap<string, string> timeKey;
	vector<string> options;
	string hosts;

	stringstream ss;
	for(size_t i = 0; i < ptr->hosts_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->hosts_list[i];
	}
	hosts = ss.str();

	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->owner));
	list.push_back(make_pair("obj_acl", ptr->acl));
	list.push_back(make_pair("aclTimeStamp", timeToString(ptr->aclTimeStamp)));
	list.push_back(make_pair("hosts", hosts));
	list.push_back(make_pair("dirty", BoolToString(ptr->dirty)));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("creationTime", timeToString(ptr->creationTime)));
	list.push_back(make_pair("lastAccessTime", timeToString(ptr->lastAccessTime)));
	list.push_back(make_pair("etag", ptr->etag));
	list.push_back(make_pair("backendProtocol", protocolToString(ptr->backendProtocol)));
	list.push_back(make_pair("bucket_name", ptr->bucket_name));
	list.push_back(make_pair("obj_name", ptr->obj_name));
	list.push_back(make_pair("home_location", homeToString(ptr->home_location)));

	   ldout(cct,10) <<"after connect" << timeToString(ptr->lastAccessTime)<<dendl;
	//creating a key entry
	keys.push_back(key);

	//making key and time a pair
	timeKey.emplace(timeToString(ptr->creationTime),key);

	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	client.rpush("objectDirectory", keys, [](cpp_redis::reply &reply){
	});

	//this will be used for aging policy
	client.zadd("keyObjectDirectory", options, timeKey, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;

}

/* the horse function to add a new key to the directory
 */
int RGWBlockDirectory::setKey(string key, cache_block *ptr){
	ldout(cct,10) <<"block after connect" << key <<dendl;
//	cpp_redis::client client;
//	client.connect("192.168.32.103", 7000);

	vector<pair<string, string>> list;
	vector<string> options;
	string hosts;

	ldout(cct,10) << __func__ << "before"<< key<< " "<<ptr->block_id  <<dendl;
	
	stringstream ss;
	for(size_t i = 0; i < ptr->hosts_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->hosts_list[i];
	}
	hosts = ss.str();

	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->c_obj.owner));
//	list.push_back(make_pair("hosts", hosts));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
//	list.push_back(make_pair("etag", ptr->etag));
	list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
	list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
	list.push_back(make_pair("block_id", to_string(ptr->block_id)));


	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();
	ldout(cct,10) << __func__ << "setValueafter" << dendl;
	return 0;

}

int RGWObjectDirectory::getValue(cache_obj *ptr){
    string key = buildIndex(ptr);

     ldout(cct,10) << __func__ << "getValue" << dendl;
    //delete the existing key,
    //to update an existing key, updateValue() should be used
    //if (RGWDirectory::existKey(key))i
    //{
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

//	cpp_redis::client client;
//	client.connect("192.168.32.103", 7000);
	
	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("obj_acl");
	fields.push_back("aclTimeStamp");
	fields.push_back("hosts");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("creationTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("home_location");


	client.hmget(key, fields, [&key, &owner, &obj_acl, &aclTimeStamp, &hosts, &dirty, &size, &creationTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name, &home_location](cpp_redis::reply& reply){
		auto arr = reply.as_array();
	      	key = arr[0].as_string();
	      	owner = arr[1].as_string();
	      	obj_acl = arr[2].as_string();
	      	aclTimeStamp = arr[3].as_string();
	      	hosts = arr[4].as_string();
	      	dirty = arr[5].as_string();
	      	size = arr[6].as_string();
	      	creationTime  = arr[7].as_string();
	      	lastAccessTime = arr[8].as_string();
	      	etag = arr[9].as_string();
	      	backendProtocol = arr[10].as_string();
	      	bucket_name = arr[11].as_string();
	      	obj_name = arr[12].as_string();
	      	home_location = arr[13].as_string();
	
	});

	client.sync_commit();
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
	ldout(cct,10) <<"after connect" << size << bucket_name << obj_name<< owner<<etag <<dendl;
	// synchronous commit, no timeout
//	client.sync_commit();
	return 0;
	/*else {
	ldout(cct,10) <<"after connect not exist" << size << bucket_name << obj_name<< owner<<etag <<dendl;
	return -1;
	}*/
}


int RGWBlockDirectory::getValue(cache_block *ptr){

    string key = buildIndex(ptr->c_obj.bucket_name, ptr->c_obj.obj_name, ptr->block_id);
    //delete the existing key,
    //to update an existing key, updateValue() should be used
    if (RGWDirectory::existKey(key)){

    string owner;
    string hosts;
    string size;
    string etag;
    string bucket_name;
    string obj_name;
	string block_id;

	cpp_redis::client client;
	client.connect("192.168.32.103", 7000);

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("hosts");
	fields.push_back("size");
	fields.push_back("etag");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("block_id");

	client.hmget(key, fields, [&key, &owner, &hosts, &size, &etag, &bucket_name, &obj_name, &block_id](cpp_redis::reply &reply){
/*
	      key = reply.as_string()[0];
	      owner = reply.as_string()[1];
	      hosts = reply.as_string()[2];
	      size = reply.as_string()[3];
	      etag = reply.as_string()[4];
  	      bucket_name = reply.as_string()[5];
	      obj_name = reply.as_string()[6];
	      block_id = reply.as_string()[7];
*/
	auto arr = reply.as_array();
      	key     = arr[0].as_string();
      	owner     = arr[1].as_string();
      	hosts     = arr[2].as_string();
      	size     = arr[3].as_string();
      	etag     = arr[4].as_string();
      	bucket_name     = arr[5].as_string();
      	obj_name     = arr[6].as_string();
      	block_id     = arr[7].as_string();

	});


	stringstream sloction(hosts);
	string tmp;

	//passing the values to the requester
	ptr->c_obj.owner = owner;

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
		ptr->hosts_list.push_back(tmp);

	ptr->size_in_bytes = stoull(size);
	ptr->etag = etag;
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	ptr->block_id = stoull(block_id);

	// synchronous commit, no timeout
	client.sync_commit();
    }
    else
	    return -1;
    return 0;
}


/* returns all the keys between startTime and endTime 
 * as a vector of <bucket_name, object_name, chunk_id, owner, creationTime> vectors
 */
vector<pair<vector<string>, time_t>> RGWObjectDirectory::get_aged_keys(time_t startTime_t, time_t endTime_t){
	vector<pair<string, string>> list;
	vector<pair<vector<string>, time_t>> keys; //return aged keys
	string key;
	string time;
	int rep_size = 0;

	string startTime = timeToString(startTime_t);
	string endTime = timeToString(endTime_t);

	cpp_redis::client client;
	client.connect("192.168.32.103", 7000);

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




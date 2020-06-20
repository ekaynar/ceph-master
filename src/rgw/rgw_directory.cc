


#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>

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


/* the following two functions should be implemented in their own respected classes */
int RGWDirectory::getValue(cache_obj *ptr){
	cout << "wrong function!";
}

int RGWDirectory::setValue(cache_obj *ptr){
	cout << "wrong function!";
}

/* builds the index for the directory
 * based on bucket_name, obj_name, and chunk_id
 */
string RGWObjectDirectory::buildIndex(cache_obj *ptr){
	return ptr->bucket_name + "_" + ptr->obj_name + "_" + to_string(ptr->chunk_id);
}

int RGWObjectDirectory::existKey(string key){
	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	int result = 0;

    vector<string> keys;
    keys.push_back(key);

	client.exists(keys, [&result](cpp_redis::reply &reply){
		result = reply.as_integer();
	});
	return result;
}

int RGWObjectDirectory::setValue(cache_obj *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
	string key = buildIndex(ptr);

	//delete the existing key, 
	//to update an existing key, updateValue() should be used
	if (existKey(key))
		delKey(key);

	return setKey(key, ptr);
	
}
int RGWObjectDirectory::setKey(string key, cache_obj *ptr){
	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	vector<pair<string, string>> list;
	vector<string> keys;
	multimap<string, string> timeKey;
	vector<string> options;
	string host;

	stringstream ss;
	for(size_t i = 0; i < ptr->host_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
			ss << ptr->host_list[i];
	}
	host = ss.str();

	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->user));
	list.push_back(make_pair("obj_acl", ptr->obj_acl));
	list.push_back(make_pair("aclTimeStamp", ptr->aclTimeStamp));
	list.push_back(make_pair("host", host));
	list.push_back(make_pair("dirty", BoolToString(ptr->dirty)));
	list.push_back(make_pair("size", std::to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("createTime", ptr->createTime));
	list.push_back(make_pair("lastAccessTime", ptr->lastAccessTime));
	list.push_back(make_pair("etag", ptr->etag));
	list.push_back(make_pair("backendProtocol", ptr->backendProtocol));
	list.push_back(make_pair("bucket_name", ptr->bucket_name));
	list.push_back(make_pair("obj_name", ptr->obj_name));

	//creating a key entry
	keys.push_back(key);

	//making key and time a pair
	timeKey.emplace(ptr->createTime,key);

	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	client.rpush("objectDirectory", keys, [](cpp_redis::reply &reply){
	});

	//this will be used for aging policy
	client.zadd("keyDirectory", options, timeKey, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;

}


int RGWObjectDirectory::getValue(cache_obj *ptr){

    string key = buildIndex(ptr);
    string owner;
    string obj_acl;
    string aclTimeStamp;
    string host;
    string dirty;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
	string chunk_id;
    string backendProtocol;
    string bucket_name;
    string obj_name;

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("obj_acl");
	fields.push_back("aclTimeStamp");
	fields.push_back("host");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("createTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");

	client.hmget(key, fields, [&key, &owner, &obj_acl, &aclTimeStamp, &host, &dirty, &size, &createTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name](cpp_redis::reply &reply){
	      key = reply.as_string()[0];
	      owner = reply.as_string()[1];
	      obj_acl = reply.as_string()[2];
	      aclTimeStamp = reply.as_string()[3];
	      host = reply.as_string()[4];
	      dirty = reply.as_string()[5];
	      size = reply.as_string()[6];
	      createTime = reply.as_string()[7];
	      lastAccessTime = reply.as_string()[8];
	      etag = reply.as_string()[9];
  		  backendProtocol = reply.as_string()[10];
  		  bucket_name = reply.as_string()[11];
	      obj_name = reply.as_string()[12];
	});

	//finding offest -> key = bucketname_objName_chunk_id
	size_t pos = key.rfind("_");
	chunk_id = key.substr(pos++);

	stringstream sloction(host);
	string tmp;

	ptr->user = owner;
	ptr->obj_acl = obj_acl;
	ptr->aclTimeStamp = aclTimeStamp;
	while(getline(sloction, tmp, '_'))
		ptr->host_list.push_back(tmp);
	ptr->dirty = StringToBool(dirty[0]);
	ptr->size_in_bytes = stoull(size);
	ptr->createTime = createTime;
	ptr->lastAccessTime = lastAccessTime;
	ptr->etag = etag;
	ptr->chunk_id = stoull(chunk_id);
	ptr->backendProtocol = backendProtocol;
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;
}

/* updatinh the directory value of host_list
 * its input is a host which has a copy of the data
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWObjectDirectory::updateHostList(cache_obj *ptr, string host){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		//updating the desired field
		tmpObj.host_list.push_back(value);

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
int RGWObjectDirectory::updateACL(cache_obj *ptr, string obj_acl){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		tmpObj.obj_acl = value;

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
int RGWObjectDirectory::updateLastAcessTime(cache_obj *ptr, ceph::real_time lastAccessTime){

	stringstream ss << lastAccessTime;
	string time = ss.str();

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		tmpObj.obj_acl = time;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}




/* updatinh the directory value*/
/* value should be string even for fields such as size or chunk_id */
/*
int RGWObjectDirectory::updateValue(cache_obj *ptr, string field, string value){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		//updating the desired field
		if (field == "user")
			tmpObj.user = value;	
		else if (field == "bucket_name")
			tmpObj.bucket_name = value;
		else if (field == "obj_name")
			tmpObj.obj_name = value;
		else if (field == "host")
			tmpObj.host_list.push_back(value);
		else if (field == "size_in_bytes")
			tmpObj.size_in_bytes = stoull(value);
		else if (field == "dirty")
			tmpObj.dirty = value[0];
		else if (field == "chunk_id")
			tmpObj.chunk_id = stoull(value);
		else if (field == "etag")
			tmpObj.etag = value;
		else if (field == "createTime")
			tmpObj.createTime = value;
		else if (field == "lastAccessTime")
			tmpObj.lastAccessTime = value;
		else if (field == "backendProtocol")
			tmpObj.backendProtocol = value;
		else if (field == "obj_acl")
			tmpObj.obj_acl = value;
		else if (field == "aclTimeStamp")
			tmpObj.aclTimeStamp = value;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		setValue(&tmpObj);
	
}
*/

int RGWObjectDirectory::delValue(cache_obj *ptr){
    string key = buildIndex(ptr);
	int result = 0;

	result += delKey(key);
	return result;
}

int RGWObjectDirectory::delKey(string key){
	int result = 0;
    vector<string> keys;
    keys.push_back(key);

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	client.del(keys, [&result](cpp_redis::reply &reply){
		result = reply.as_integer();
	});
	return result;
}


//returns all the keys between startTime and endTime as <key, time> paris
/*
std::vector<std::pair<std::string, std::string>> RGWDirectory::get_aged_keys(string startTime, string endTime){
	std::vector<std::pair<std::string, std::string>> list;
	std::string key;
	std::string time;

	cpp_redis::client client;
	//	//client.connect();
	client.connect("127.0.0.1", 7000);

	std::string dirKey = "main_directory";
	client.zrangebyscore(dirKey, startTime, endTime, true, [&key, &time](cpp_redis::reply &reply){
	      for (unsigned i = 0; i < reply.as_array().size(); i+=2)
	      {
	          key = reply.as_string()[i];
	          time = reply.as_string()[i+1];

	      }
	      //if (reply.is_error() == false)
  });

	// synchronous commit, no timeout
	client.sync_commit();

	return list;
}

*/



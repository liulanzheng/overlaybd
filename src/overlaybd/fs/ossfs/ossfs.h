#pragma once

#include <inttypes.h>
#include <dirent.h>

namespace AlibabaCloud
{
    namespace OSS
    {
        class OssClient;
    }
}

#define OSS_MAGIC 0xE32DF01A
#define O_MULTI_PART 0x40000000
namespace FileSystem {

struct ossitem : public ::dirent
{
    int64_t st_size;
};

extern "C"
{
    int ossfs_init();
    class IAsyncFileSystem;
    IAsyncFileSystem* new_ossfs_from_client(AlibabaCloud::OSS::OssClient* client, bool ownership = false);
    IAsyncFileSystem* new_ossfs(const char* region, const char* bucket,
                                const char* key, const char* key_secret,
                                uint64_t request_timeout = 10000UL * 1000,
                                uint64_t connect_timeout = 5000UL * 1000,
                                uint64_t maxconn = 16UL);
    int ossfs_fini();
    // set oss log level via ALOG level. set level to -1 can set log off
    // oss log is off in default
    void ossfs_setloglevel(int level=5);
}}


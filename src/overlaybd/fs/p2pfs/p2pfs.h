#pragma once
#include "../filesystem.h"
#include "client.h"
#include "root_selector.h"

namespace FileSystem
{

    IFileSystem* new_p2pfs(
        RootSelector* root_selector,
        const NodeID &myid = NodeID(), //自身Client的ID
        IFileSystem* metafs = nullptr, //用于校验层。如果不需要校验则为nullptr
        ConnectionTracer* tracer = nullptr,
        bool balanced = false,
        int ttl = 200,
        int retry_time = 5,
        const char* domain = nullptr,
        uint64_t connect_timeout = 1000UL * 1000,
        uint64_t rpc_timeout = -1,
        bool root_ssl = false
    );
}
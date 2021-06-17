#pragma once
#include <stddef.h>
#include <inttypes.h>
#include <functional>
#include <unistd.h>
#include <sys/stat.h>
#include "../../rpc/serialize.h"
#include "../../net/socket.h"

namespace FileSystem
{
    struct NodeID
    {
        uint32_t addr = 0;
        uint16_t port = 0;
        uint16_t randomizer = (uint16_t)rand();
        NodeID() = default;
        NodeID(Net::EndPoint ep) :
            addr(ep.addr.to_nl()), port(ep.port) { }
        Net::EndPoint endpoint() const
        {
            return Net::EndPoint{Net::IPAddr(addr), port};
        }
        uint64_t value() const
        {
            return *(uint64_t*)this;
        }
        bool operator==(const NodeID& rhs) const {
            return value() == rhs.value();
        }
        bool operator!=(const NodeID& rhs) const {
            return !(*this == rhs);
        }
    };

    inline LogBuffer& operator << (LogBuffer& log, const NodeID ep) { return log << ep.endpoint(); }

    static_assert(sizeof(NodeID) == sizeof(uint64_t), "...");

    const static uint32_t IID_P2P = 0x8096;

    struct P2P_Interface
    {
        const static uint32_t IID = IID_P2P;
    };

    // sent from child nodes periodically
    struct Heartbeat : public P2P_Interface
    {
        const static uint32_t FID = 0;
        struct Request : public RPC::Message
        {
            NodeID id;          // client id
            uint64_t timestamp; // current time
            PROCESS_FIELDS(id, timestamp);
        };
        struct Response : public RPC::Message
        {
            NodeID id;          // server id
            uint64_t timestamp; // the timestamp from Request
            PROCESS_FIELDS(id, timestamp);
        };
    };

    struct P2PReadV : public P2P_Interface
    {
        const static uint32_t FID = 1;
        struct Request : public RPC::Message
        {
            RPC::string filename;
            RPC::string domain;
            off_t offset;
            size_t count;
            NodeID id;
            int ttl;

            RPC::array<NodeID> blacklist;   // try NOT to redirect me to these nodes

            PROCESS_FIELDS(filename, domain, offset, count, id, ttl, blacklist);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;        // size actually read, or -errno in case of error;
                                // ret == -EAGAIN indicates server currently has no data,
                                //     and the client should try again some time later;
                                // ret == -EBUSY indicates redirection;
            NodeID serverid;    // server id
            NodeID redirect2;   // redirect to this node, if needed

            RPC::aligned_iovec_array buf;  // may contain error string in case of error

            PROCESS_FIELDS(ret, serverid, redirect2, buf);
        };
    };

    // cache protocols are direct to one node, therefore no need of redirection
    // result of cache hit query use 0 for yes and -1 for no as errornumber
    struct P2PCacheHit : public P2P_Interface
    {
        const static uint32_t FID = 2;
        struct Request: public RPC::Message
        {
            RPC::string filename;
            off_t offset;
            size_t count;

            PROCESS_FIELDS(filename, offset, count);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;

            PROCESS_FIELDS(ret);
        };
    };

    // ask for prefetch will return till it is fetched, basiclly it is a pread,
    // but no need to send back the result bytes.
    struct P2PPrefetch : public P2P_Interface
    {
        const static uint32_t FID = 3;
        struct Request: public RPC::Message
        {
            RPC::string filename;
            off_t offset;
            size_t count;

            PROCESS_FIELDS(filename, offset, count);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;

            PROCESS_FIELDS(ret);
        };
    };

    // ask for prefetch will return till it is fetched, basiclly it is a pread,
    // but no need to send back the result bytes.
    struct P2PStat : public P2P_Interface
    {
        const static uint32_t FID = 4;
        struct Request: public RPC::Message
        {
            RPC::string filename;

            PROCESS_FIELDS(filename);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;
            struct stat stat;

            PROCESS_FIELDS(ret, stat);
        };
    };

    // EvictKeys will return result immediately which indicate launch state of eviction
    // , eviction process will be executed asynchronously.
    struct P2PEvictKeys : public P2P_Interface
    {
        const static uint32_t FID = 5;
        struct Request: public RPC::Message
        {
            RPC::string prefix;

            PROCESS_FIELDS(prefix);
        };
        struct Response : public RPC::Message
        {
            int ret;

            PROCESS_FIELDS(ret);
        };
    };

    struct P2PWriteV : public P2P_Interface
    {
        const static uint32_t FID = 6;
        struct Request : public RPC::Message
        {
            RPC::string filename;
            RPC::aligned_iovec_array buf;
            off_t offset;

            PROCESS_FIELDS(filename, buf, offset);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;        // size actually write, or -errno in case of error;
                                // ret == -EAGAIN indicates server currently has no space,
                                //     and the client should try again some time later;

            RPC::string error_string;  // may contain error string in case of error

            PROCESS_FIELDS(ret, error_string);
        };
    };

    struct P2PEvict : public P2P_Interface
    {
        const static uint32_t FID = 7;
        struct Request : public RPC::Message
        {
            RPC::string filename;
            off_t offset;
            size_t count;

            PROCESS_FIELDS(filename, offset, count);
        };
        struct Response : public RPC::Message
        {
            ssize_t ret;

            PROCESS_FIELDS(ret);
        };
    };

    struct P2PRename : public P2P_Interface
    {
        const static uint32_t FID = 8;
        struct Request: public RPC::Message
        {
            RPC::string oldname;
            RPC::string newname;

            PROCESS_FIELDS(oldname, newname);
        };
        struct Response : public RPC::Message
        {
            int ret;
            NodeID redirect2;   // redirect to this node, if needed
            PROCESS_FIELDS(ret, redirect2);
        };
    };

    struct P2PTruncate : public P2P_Interface
    {
        const static uint32_t FID = 9;
        struct Request: public RPC::Message
        {
            RPC::string filename;
            off_t length;

            PROCESS_FIELDS(filename, length);
        };
        struct Response : public RPC::Message
        {
            int ret;

            PROCESS_FIELDS(ret);
        };
    };

    struct P2PAnnounceLatest : public P2P_Interface
    {
        const static uint32_t FID = 20;
        struct Request : public RPC::Message
        {
            RPC::string filename;
            Net::EndPoint owner_ep;
            uint64_t version;

            PROCESS_FIELDS(filename, owner_ep, version);
        };

        struct Response : public RPC::Message
        {
            /** ret = 0 announce success
             *  ret = -EINVAL means version is Expired
             */
            ssize_t ret;

            PROCESS_FIELDS(ret);
        };
    };
    struct P2PReqLatest : public P2P_Interface
    {
        const static uint32_t FID = 21;
        struct Request : public RPC::Message
        {
            RPC::string filename;

            PROCESS_FIELDS(filename);
        };

        struct Response : public RPC::Message
        {
            ssize_t ret;
            Net::EndPoint server_ep;
            uint64_t version;

            PROCESS_FIELDS(ret, server_ep, version);
        };
    };

    struct P2PStat_V2 : public P2PStat
    {
        const static uint32_t FID = 41;

        struct Response : public P2PStat::Response
        {
            NodeID redirect2;   // redirect to this node, if needed

            PROCESS_FIELDS(ret, stat, redirect2);
        };
    };
}

namespace std{
    template<>
    struct hash<FileSystem::NodeID>
    {
        size_t operator() (const FileSystem::NodeID& a) const
        {
            return std::hash<uint64_t>()(a.value());
        }
    };
}

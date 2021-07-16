#include "ossfs.h"
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>
#include <fcntl.h>
#include <string>
#include <memory>
#include <mutex>
#include <alibabacloud/oss/OssClient.h>
#include "../async_filesystem.h"
#include "../../iovector.h"
#include "../../alog.h"
#include "../../alog-stdstring.h"
#include "../../utility.h"
#include "../../expirecontainer.h"
using namespace std;
using namespace AlibabaCloud::OSS;

namespace FileSystem
{
    template<typename T>
    struct AsyncContext : public AlibabaCloud::OSS::AsyncCallerContext
    {
        Done<T> done;
        AsyncResult<T> result;
        ~AsyncContext()
        {
            done(&result);      // trigger callback when destruct
        }
        template<typename OUTCOME>
        int judge_error(const OUTCOME& outcome)
        {
            if (outcome.isSuccess())
                return result.error_number = 0;

            result.result = -1;
            result.error_number = EIO;
            auto& e = outcome.error();
            auto& Code = e.Code();
            auto& Message = e.Message();
            auto& RequestID = e.RequestId();
            auto& Host = e.Host();
            LOG_ERROR("failed to perform OSS::GetObject ",
                VALUE(Code), VALUE(Message), VALUE(RequestID), VALUE(Host));
            return -1;
        }
    };

    template<typename Request, typename Outcome, typename Context>
    static void async_op_done(const OssClient*, const Request&,
        const Outcome& outcome, const std::shared_ptr<const AsyncCallerContext>& context)
    {
        auto ctx = (Context*)context.get();
        ctx->give_result(outcome);
    }

    static inline ossitem init_ossitem(long ino, unsigned char type, string_view name, int64_t size)
    {
        ossitem item;
        item.d_ino = ino;
        item.d_type = type;
        auto len = std::min(name.size(), sizeof(item.d_name) - 1);
        memcpy(item.d_name, name.data(), len);
        item.d_name[len] = '\0';
        item.st_size = size;
        return std::move(item);
    }

    class ossfs;
    class ossfile;
    static IAsyncFile* new_oss_file(ossfs*, string&&, string&&, int);
    static AsyncDIR* new_oss_dir(std::vector<ossitem>&&);
    class ossfs : public IAsyncFileSystem
    {
    public:
        OssClient* m_client;
        std::string m_bucket;
        uint64_t m_refcnt = 0;
        bool m_ownership;
        ossfs(OssClient* client, bool ownership) :
            m_client(client), m_ownership(ownership) { }
        ossfs(const char* region, const char* bucket, const char* key,
              const char* key_secret, uint64_t req_timeout,
              uint64_t conn_timeout, uint64_t maxconn) {
            ClientConfiguration conf;  // using default configuration
            conf.requestTimeoutMs = req_timeout / 1000;
            conf.connectTimeoutMs = conn_timeout / 1000;
            conf.maxConnections = maxconn;
            m_client = new OssClient(region, key, key_secret, conf);
            m_bucket = bucket;
            LOG_INFO("new ossfs created ` `", region, bucket);
        }
        virtual ~ossfs()
        {
            assert(m_refcnt == 0);
            if (m_ownership)
                delete m_client;
        }
        void addref()
        {
            m_refcnt++;
        }
        void rmref()
        {
            m_refcnt--;
        }
        int parse_names(const char* path, string& bucket_name, string& object_name)
        {
            if (!path)
                return -1;
            int prefix_padding = path[0] == '/' ? 1:0;
            if (!m_bucket.empty()) { // path is object name
              bucket_name.assign(m_bucket);
              object_name.assign(path + prefix_padding);
            } else {
              // eg : /suoshi-yf/ease_test_oss; suoshi-yf is bucket, ease_test_oss is object
              auto slashPos = strchr(path + prefix_padding, '/');
              if (!slashPos) {
                  return -1;
              }
              bucket_name.assign(path + prefix_padding, slashPos);
              object_name.assign(slashPos + 1);
            }
            return 0;
        }
        EXPAND_FUNC(IAsyncFile*, do_open_stat, string&& bucket_name,
                    string&& object_name, ossfile* file);
        EXPAND_FUNC(IAsyncFile*, open, const char* pathname, int flags)
        override {
            LOG_INFO("open `", pathname);
            string bucket_name, object_name;
            int ret = parse_names(pathname, bucket_name, object_name);
            if (ret < 0 || object_name.empty())
                return callback(done, OPID_OPEN, (IAsyncFile*)nullptr, EINVAL);

            auto file = (ossfile*)new_oss_file(this, std::string(bucket_name),
                                               std::string(object_name), flags);
            do_open_stat(std::move(bucket_name), std::move(object_name), file, done, timeout);
        }
        EXPAND_FUNC(IAsyncFile*, open, const char *pathname, int flags, mode_t mode) override
        {
            LOG_INFO("open ossfs `", pathname);
            open(pathname, flags, done, timeout);
        }
        int stat_for_directory(const string& bucket_name, const string& object_name, struct stat *buf) {
            ListObjectsRequest req(bucket_name);
            if (object_name.back() != '/')
                req.setPrefix(object_name + '/');
            else
                req.setPrefix(object_name);
            req.setMaxKeys(1);
            auto lsoutcome = m_client->ListObjects(req);
            if (!lsoutcome.isSuccess()) // failed to query
                return -EIO;
            auto result = lsoutcome.result();
            if (result.ObjectSummarys().size() > 0) { // it is a directory
                if (buf) {
                    memset(buf, 0, sizeof(*buf));
                    buf->st_mode = (S_IFDIR | 0755);
                }
                return 0;
            }
            return -ENOENT;
        }
        int stat_for_file(string&& bucket_name, string&& object_name, struct stat *buf) {
            auto outcome = m_client->GetObjectMeta(bucket_name, object_name);
            if (!outcome.isSuccess()) {
                if (outcome.error().Code() == "ServerError:404") {
                    return -ENOENT;
                }
                LOG_ERROR("GetObjectMeta bucket ` object ` fail, ` `",
                    bucket_name, object_name, outcome.error().Code(), outcome.error().Message());
                return -EIO;
            }
            if (buf) {
                memset(buf, 0, sizeof(*buf));
                buf->st_mode = (S_IFREG | 0644);
                buf->st_size = outcome.result().ContentLength();
            }
            return 0;
        }
        int stat_for_bucket(std::string&& bucket_name, struct stat *buf) {
            auto bioutcome = m_client->GetBucketInfo(bucket_name);
            if (!bioutcome.isSuccess())
                return -EIO;
            if (buf) {
                memset(buf, 0, sizeof(*buf));
                buf->st_mode = (S_IFDIR | 0755);
            }
            return 0;
        }
        int do_stat_sync(string&& bucket_name, string&& object_name,
                         struct stat* buf) {
            if (object_name.length() == 0) {  // bucket
                return stat_for_bucket(std::move(bucket_name), buf);
            }
            if (object_name.back() == '/') {  // consider as it is a directory
                return stat_for_directory(std::move(bucket_name), std::move(object_name), buf);
            }
            // or it might be file , or directory
            // sinc using rval reference, here must delievered a reference
            int ret = stat_for_file(std::string(bucket_name), std::string(object_name), buf);
            if (ret < 0) {  // so it is not file, consider directory
                ret = stat_for_directory(std::move(bucket_name), std::move(object_name), buf);
                return ret;
            }
            // so it is file and have success.
            return ret;
        }
        void do_stat(uint32_t OP, string&& bucket_name, string&& object_name,
                     struct stat* buf, Done<int> done, uint64_t timeout = -1) {
            std::thread(
                [this, OP](string&& bucket_name, string&& object_name,
                       struct stat* buf, Done<int> done) {
                    //LOG_INFO("do_stat ` `", bucket_name, object_name);
                    int ret = do_stat_sync(std::move(bucket_name),
                                           std::move(object_name), buf);
                    if (ret < 0) return callback(done, OP, -1, -ret);
                    return callback(done, OP, 0, 0);
                },
                std::move(bucket_name), std::move(object_name), buf, done)
                .detach();
        }
        EXPAND_FUNC(int, stat, const char* pathname, struct stat* buf) override {
            std::string bucket_name, object_name;
            int ret = parse_names(pathname, bucket_name, object_name);
            if (ret < 0) return callback(done, OPID_STAT, -1, EINVAL);
            do_stat(OPID_STAT, std::move(bucket_name), std::move(object_name), buf, done,
                                     timeout);
        }
        OVERRIDE_ASYNC(IAsyncFile*, creat, const char *pathname, mode_t mode)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, mkdir, const char *pathname, mode_t mode)
        {
            std::thread([=]{
                string bucket_name, dir_name;
                int ret = parse_names(pathname, bucket_name, dir_name);
                if (ret < 0)
                    return callback(done, OPID_MKDIR, -1, EINVAL);
                if (dir_name.back() != '/') dir_name += '/';
                std::shared_ptr<std::iostream> content = std::make_shared<std::stringstream>();
                PutObjectRequest request(bucket_name, dir_name, content);
                auto outcome = m_client->PutObject(request);
                if (!outcome.isSuccess())
                    return callback(done, OPID_MKDIR, -1, EIO);;
                callback(done, OPID_MKDIR, 0, 0);
            }).detach();
        }
        OVERRIDE_ASYNC(int, rmdir, const char *pathname)
        {
            std::thread([=]{
                string bucket_name, dir_name;
                int ret = parse_names(pathname, bucket_name, dir_name);
                if (ret < 0)
                    return callback(done, OPID_RMDIR, -1, EINVAL);
                if (!dir_name.empty() && dir_name.back() != '/') dir_name += '/';
                ListObjectOutcome outcome;
                std::string nextMarker = "";
                do {
                    ListObjectsRequest request(bucket_name);
                    request.setPrefix(dir_name);
                    request.setMarker(nextMarker);
                    request.setMaxKeys(1000);
                    outcome = m_client->ListObjects(request);
                    if (!outcome.isSuccess())
                        return callback(done, OPID_RMDIR, -1, EIO);
                    DeleteObjectsRequest dRequest(bucket_name);
                    for (const auto& obj : outcome.result().ObjectSummarys()) {
                        dRequest.addKey(obj.Key());
                    }

                    auto del = m_client->DeleteObjects(dRequest);
                    if (!del.isSuccess())
                        return callback(done, OPID_RMDIR, -1, EIO);
                    nextMarker = outcome.result().NextMarker();
                } while (outcome.result().IsTruncated());

                callback(done, OPID_RMDIR, 0, 0);
            }).detach();
        }
        OVERRIDE_ASYNC(int, symlink, const char *oldname, const char *newname)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(ssize_t, readlink, const char *path, char *buf, size_t bufsiz)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, link, const char *oldname, const char *newname)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, rename, const char *oldname, const char *newname)
        {
            std::thread([=]{
                string bucket_old, object_old, bucket_new, object_new;
                int ret = parse_names(oldname, bucket_old, object_old);
                if (ret < 0)
                    return callback(done, OPID_RENAME, -1, EINVAL);
                ret = parse_names(newname, bucket_new, object_new);
                if (ret < 0)
                    return callback(done, OPID_RENAME, -1, EINVAL);
                struct stat buf;
                ret = do_stat_sync(std::string(bucket_old), std::string(object_old), &buf);
                if (ret < 0)
                    return callback(done, OPID_RENAME, -1, -ret);
                if (bucket_old == bucket_new && object_old == object_new)
                    return callback(done, OPID_RENAME, 0, 0);
                if (buf.st_mode & S_IFDIR) {
                    if (!object_old.empty() && object_old.back() != '/') object_old += '/';
                    if (!object_new.empty() && object_new.back() != '/') object_new += '/';
                    if (bucket_old == bucket_new && object_old == object_new)
                        return callback(done, OPID_RENAME, 0, 0);
                    std::vector<std::string> delKeys;
                    ListObjectOutcome outcome;
                    std::string nextMarker = "";
                    do {
                        ListObjectsRequest request(bucket_old);
                        request.setPrefix(object_old);
                        request.setMarker(nextMarker);
                        request.setMaxKeys(1000);
                        outcome = m_client->ListObjects(request);
                        if (!outcome.isSuccess())
                            return callback(done, OPID_RENAME, -1, EIO);
                        for (const auto& obj : outcome.result().ObjectSummarys()) {
                            string_view name(obj.Key());
                            name.remove_prefix(object_old.size());
                            CopyObjectRequest request(bucket_new, object_new + std::string(name.data(), name.size()));
                            request.setCopySource(bucket_old, obj.Key());
                            auto copy = m_client->CopyObject(request);
                            if (!copy.isSuccess())
                                return callback(done, OPID_RENAME, -1, EIO);
                            delKeys.emplace_back(obj.Key());
                        }
                        nextMarker = outcome.result().NextMarker();
                    } while (outcome.result().IsTruncated());

                    DeleteObjectsRequest dRequest(bucket_old);
                    for(size_t i = 1; i <= delKeys.size(); ++i){
                        dRequest.addKey(delKeys[i - 1]);
                        if((i % 1000 == 0) || (i == delKeys.size())){
                            auto del = m_client->DeleteObjects(dRequest);
                            if (!del.isSuccess())
                                return callback(done, OPID_RENAME, -1, EIO);
                            dRequest.clearKeyList();
                        }
                    }
                } else {
                    CopyObjectRequest cRequest(bucket_new, object_new);
                    cRequest.setCopySource(bucket_old, object_old);
                    auto copy = m_client->CopyObject(cRequest);
                    if (!copy.isSuccess())
                        return callback(done, OPID_RENAME, -1, EIO);
                    DeleteObjectRequest dRequest(bucket_old, object_old);
                    auto del = m_client->DeleteObject(dRequest);
                    if (!del.isSuccess())
                        return callback(done, OPID_RENAME, -1, EIO);
                }

                callback(done, OPID_RENAME, 0, 0);
            }).detach();
        }

        int init_file(string&& bucket_name, string&& object_name, ssize_t size, int flags, string* upload_id, int* part_number) {
            if (size == 0 && (flags & O_APPEND)) {
                std::shared_ptr<std::iostream> content = std::make_shared<std::stringstream>();
                AppendObjectRequest request(bucket_name, object_name, content);
                request.setPosition(0);
                auto outcome = m_client->AppendObject(request);
                if (!outcome.isSuccess())
                    return -EIO;
            } else if (size == 0) {
                std::shared_ptr<std::iostream> content = std::make_shared<std::stringstream>();
                PutObjectRequest request(bucket_name, object_name, content);
                auto outcome = m_client->PutObject(request);
                if (!outcome.isSuccess())
                    return -EIO;
            }

            if (size == 0 && (flags & O_MULTI_PART)) {
                std::vector<std::string> aborts;
                ListMultipartUploadsRequest lRequest(bucket_name);
                lRequest.setMaxUploads(1000);
                lRequest.setPrefix(object_name);
                ListMultipartUploadsOutcome lResult;
                do {
                    lResult = m_client->ListMultipartUploads(lRequest);
                    if (!lResult.isSuccess())
                        break;
                    for (const auto& part : lResult.result().MultipartUploadList()) {
                        LOG_INFO("abort part ` ` `", part.Key, part.UploadId, part.Initiated);
                        aborts.emplace_back(part.UploadId);
                    }
                    lRequest.setKeyMarker(lResult.result().NextKeyMarker());
                    lRequest.setUploadIdMarker(lResult.result().NextUploadIdMarker());
                } while (lResult.result().IsTruncated());
                for (auto& abrt : aborts) {
                    AbortMultipartUploadRequest aRequest(bucket_name, object_name, abrt);
                    (void)m_client->AbortMultipartUpload(aRequest);
                }
                InitiateMultipartUploadRequest request(bucket_name, object_name);
                auto outcome = m_client->InitiateMultipartUpload(request);
                if (!outcome.isSuccess()) {
                    LOG_ERROR("InitiateMultipartUpload bucket ` object ` fail, ` `",
                        bucket_name, object_name, outcome.error().Code(), outcome.error().Message());
                    return -EIO;
                }
                *upload_id = outcome.result().UploadId();
                *part_number = 1;
            }
            return 0;
        }
        int delete_file(string&& bucket_name, string&& object_name) {
            DeleteObjectRequest request(bucket_name, object_name);
            auto outcome = m_client->DeleteObject(request);
            if (!outcome.isSuccess()) {
                if (outcome.error().Code() == "ServerError:404") {
                    return 0;
                }
                return -EIO;
            }
            return 0;
        }
        OVERRIDE_ASYNC(int, unlink, const char *filename)
        {
            std::thread([=]{
                string bucket_name, object_name;
                int ret = parse_names(filename, bucket_name, object_name);
                if (ret < 0 || object_name.empty())
                    return callback(done, OPID_UNLINK, -1, EINVAL);
                ret = delete_file(std::move(bucket_name), std::move(object_name));
                if (ret < 0)
                    return callback(done, OPID_UNLINK, -1, -ret);
                callback(done, OPID_UNLINK, 0, 0);
            }).detach();
        }
        OVERRIDE_ASYNC(int, chmod, const char *pathname, mode_t mode)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, chown, const char *pathname, uid_t owner, gid_t group)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, lchown, const char *pathname, uid_t owner, gid_t group)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, statfs, const char *path, struct statfs *buf)
        {
            (void)path;
            buf->f_type = OSS_MAGIC;
            callback(done, OPID_STATFS, 0, 0);
        }
        OVERRIDE_ASYNC(int, statvfs, const char *path, struct statvfs *buf)
        {
            // model ossfs, avoid too many error messages.
            // 96TiB
            memset(buf, 0, sizeof(struct statvfs));
            buf->f_bsize  = 4 * 1024u;
            buf->f_blocks = 96ul * 1024 * 1024 * 1024 * 256;
            buf->f_bfree  = buf->f_blocks;
            buf->f_bavail = buf->f_blocks;
            buf->f_namemax = NAME_MAX;
            callback(done, OPID_STATVFS, 0, 0);
        }
        // OVERRIDE_ASYNC(int, stat, const char *path, struct stat *buf)
        // {
        //     callback_umimplemented(done);
        // }
        OVERRIDE_ASYNC(int, lstat, const char *path, struct stat *buf)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, access, const char *pathname, int mode)
        {
            string bucket_name, object_name;
            int ret = parse_names(pathname, bucket_name, object_name);
            if (ret < 0)
                return callback(done, OPID_ACCESS, -1, EINVAL);
            do_stat(OPID_ACCESS, std::move(bucket_name), std::move(object_name), nullptr, done, timeout);
        }
        OVERRIDE_ASYNC(int, truncate, const char *path, off_t length)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC0(int, syncfs)
        {
            callback_umimplemented(done);
        }

        void do_open_directory(const string& bucket_name, const string& dir_name,
            Done<AsyncDIR*> done, uint64_t timeout)
        {
            auto dir = dir_name;
            if (!dir.empty() && dir.back() != '/')
                dir += '/';
            std::vector<ossitem> items;
            items.emplace_back(init_ossitem(0, DT_DIR, ".", 0));
            items.emplace_back(init_ossitem(0, DT_DIR, "..", 0));
            ListObjectOutcome outcome;
            std::string nextMarker = "";
            do {
              ListObjectsRequest req(bucket_name);
              req.setPrefix(dir);
              req.setDelimiter("/");
              req.setMarker(nextMarker);
              req.setMaxKeys(1000);
              outcome = m_client->ListObjects(req);
              if (!outcome.isSuccess())
                  return callback(done, OPID_OPENDIR, (AsyncDIR*)nullptr, EIO);
              LOG_DEBUG("open ` `", bucket_name, dir);
              for (const auto& object : outcome.result().ObjectSummarys()) {
                  LOG_DEBUG("get object `", object.Key());
                  string_view name(object.Key());
                  name.remove_prefix(dir.size());
                  if (name.empty()) continue;
                  items.emplace_back(init_ossitem(0, DT_REG, name, object.Size()));
              }
              for (const auto& commonPrefix : outcome.result().CommonPrefixes()) {
                  LOG_DEBUG("get prefix `", commonPrefix);
                  string_view name(commonPrefix);
                  name.remove_prefix(dir.size());
                  if (name.empty()) continue;
                  name.remove_suffix(1);    // remove last '/'
                  items.emplace_back(init_ossitem(0, DT_DIR, name, 0));
              }
              nextMarker = outcome.result().NextMarker();
            } while (outcome.result().IsTruncated());

            callback(done, OPID_OPENDIR, new_oss_dir(std::move(items)), 0);
        }

        OVERRIDE_ASYNC(AsyncDIR*, opendir, const char *name)
        {
            std::thread([=]{
                string bucket_name, dir_name;
                int ret = parse_names(name, bucket_name, dir_name);
                if (ret < 0)
                    return callback(done, OPID_OPENDIR, (AsyncDIR*)nullptr, EINVAL);
                do_open_directory(bucket_name, dir_name, done, timeout);
            }).detach();
        }
    };

    class ossfile : public IAsyncFile
    {
    public:
        ossfs* m_fs;
        string bucket_name, object_name;
        struct stat m_stat = {0};
        int open_flags;
        string upload_id;
        int part_result = 0;
        int part_number = 0;
        map<int, Part> part_map;
        ossfile(ossfs* fs, string&& bucket, string&& object, int flags) :
            m_fs(fs), bucket_name(std::move(bucket)), object_name(std::move(object)), open_flags(flags)
        {
            m_fs->addref();
            m_stat.st_size = -1;
        }
        virtual ~ossfile()
        {
            m_fs->rmref();
        }
        virtual IAsyncFileSystem* filesystem() override
        {
            return m_fs;
        }
        struct AsyncPReadContext : public AsyncContext<ssize_t>
        {
            void* buf;
            size_t count;
            void give_result(const GetObjectOutcome& outcome)
            {
                if (judge_error(outcome) == 0)
                {
                    auto len = outcome.result().Metadata().ContentLength();
                    if (len > 0)
                    {
                        if (len > static_cast<int64_t>(count))
                            len = count;
                        outcome.result().Content()->read((char*)buf, len);
                    }
                    result.result = len;
                }
            }
        };
        //  !!! when [offset] or [offset + count - 1] is beyond oss object size,
        //  oss will return the whole object.
        EXPAND_FUNC(ssize_t, pread, void *buf, size_t count, off_t offset) override
        {
            if (m_stat.st_size >= 0) {
                if (offset >= m_stat.st_size)
                    return callback(done, OPID_PREAD, (ssize_t)0, 0);
                if (offset + (off_t)count > m_stat.st_size)
                    count = m_stat.st_size - offset;
            }
            GetObjectRequest request(bucket_name, object_name);
            request.setRange(offset, offset + count - 1);
            auto context = make_shared<AsyncPReadContext>();
            context->buf = buf;
            context->count = count;
            context->done = done;
            context->result.object = this;
            context->result.operation = OPID_PREAD;
            auto handler = &async_op_done<GetObjectRequest, GetObjectOutcome, AsyncPReadContext>;
            m_fs->m_client->GetObjectAsync(request, handler, context);  // TODO: timeout
        }

        struct AsyncPReadVContext : public AsyncContext<ssize_t>
        {
            AsyncPReadVContext(const struct iovec *iov, int iovcnt)
                : iovec(iov, iovcnt) {
            }
            IOVector iovec;
            size_t count;
            void give_result(const GetObjectOutcome& outcome)
            {
                if (judge_error(outcome) == 0)
                {
                    auto len = outcome.result().Metadata().ContentLength();
                    if (len > 0)
                    {
                        if (len > static_cast<int64_t>(count))
                            len = count;
                        auto size = len;
                        auto& f = outcome.result().Content();
                        for (auto& v: iovec.view())
                        {
                            if (size > static_cast<int64_t>(v.iov_len)) {
                                f->read((char*)v.iov_base, v.iov_len);
                                size -= v.iov_len;
                            } else {
                                f->read((char*)v.iov_base, size);
                                size = 0;
                                break;
                            }
                        }
                    }
                    result.result = len;
                }
            }
        };
        EXPAND_FUNC(ssize_t, preadv, const struct iovec *iov, int iovcnt, off_t offset) override
        {
            auto context = make_shared<AsyncPReadVContext>(iov, iovcnt);
            auto count = context->iovec.sum();
            if (m_stat.st_size >= 0) {
                if (offset >= m_stat.st_size)
                    return callback(done, OPID_PREADV, (ssize_t)0, 0);
                if (offset + (off_t)count > m_stat.st_size) {
                    count = m_stat.st_size - offset;
                    context->iovec.shrink_to(count);
                }
            }
            GetObjectRequest request(bucket_name, object_name);
            request.setRange(offset, offset + count - 1);
            context->count = count;
            context->done = done;
            context->result.object = this;
            context->result.operation = OPID_PREADV;
            auto handler = &async_op_done<GetObjectRequest, GetObjectOutcome, AsyncPReadVContext>;
            m_fs->m_client->GetObjectAsync(request, handler, context);  // TODO: timeout
        }
        EXPAND_FUNC(int, fstat, struct stat *buf) override
        {
            if (m_stat.st_size > 0 && !(open_flags & O_APPEND)) {
                memcpy(buf, &m_stat, sizeof(struct stat));
                return callback(done, OPID_FSTAT, 0, 0);
            }
            std::thread(
                [this](struct stat *buf, Done<int> done) {
                    //LOG_INFO("fstat ` `", bucket_name, object_name);
                    int ret = m_fs->stat_for_file(std::string(bucket_name),
                                                  std::string(object_name), &m_stat);
                    if (ret < 0)
                        return callback(done, OPID_FSTAT, -1, -ret);
                    memcpy(buf, &m_stat, sizeof(struct stat));
                    return callback(done, OPID_FSTAT, 0, 0);
                },
                buf, done)
                .detach();
        }
        OVERRIDE_ASYNC(ssize_t, read, void *buf, size_t count)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(ssize_t, readv, const struct iovec *iov, int iovcnt)
        {
            callback_umimplemented(done);
        }

        int do_write(std::shared_ptr<std::iostream> content, int pn, size_t count)
        {
            if (open_flags & O_MULTI_PART) {
                if (upload_id.empty()) {
                    LOG_ERROR("Upload id is null, bucket ` object ` part `", bucket_name, object_name, pn);
                    return -EPERM;
                }
                if (part_result < 0) {
                    LOG_ERROR("Previous upload failed `, bucket ` object ` upload id ` part `",
                        part_result, bucket_name, object_name, upload_id, pn);
                    return -EIO;
                }
                UploadPartRequest request(bucket_name, object_name, content);
                request.setContentLength(count);
                request.setUploadId(upload_id);
                request.setPartNumber(pn);
                //LOG_INFO("Begin upload `", pn);
                auto outcome = m_fs->m_client->UploadPart(request);
                //LOG_INFO("End upload `", pn);
                if (!outcome.isSuccess()) {
                    LOG_ERROR("UploadPartRequest bucket ` object ` part ` fail, ` `",
                        bucket_name, object_name, pn, outcome.error().Code(), outcome.error().Message());
                    part_result = -1;
                    return -EIO;
                }
                Part part(pn, outcome.result().ETag());
                part_map[pn] = std::move(part);
            } else if (open_flags & O_APPEND) {
                AppendObjectRequest request(bucket_name, object_name, content);
                request.setPosition(m_stat.st_size);
                auto outcome = m_fs->m_client->AppendObject(request);
                if (!outcome.isSuccess())
                    return -EIO;
            } else {
                PutObjectRequest request(bucket_name, object_name, content);
                auto outcome = m_fs->m_client->PutObject(request);
                if (!outcome.isSuccess())
                    return -EIO;
            }
            return 0;
        }

        void do_write(uint32_t OP, const void *buf, size_t count, Done<ssize_t> done)
        {
            //NOT_IMPLEMENTED
            return callback(done, OP, (ssize_t)0, 0);
        }

        EXPAND_FUNC(ssize_t, write, const void *buf, size_t count) override
        {
            do_write(OPID_WRITE, buf, count, done);
        }

        void do_writev(uint32_t OP, const struct iovec *iov, int iovcnt , Done<ssize_t> done)
        {
            // NOT_IMPLEMENTED
            return callback(done, OP, (ssize_t)0, 0);
        }

        EXPAND_FUNC(ssize_t, writev, const struct iovec *iov, int iovcnt) override
        {
            do_writev(OPID_WRITEV, iov, iovcnt, done);
        }

        OVERRIDE_ASYNC(ssize_t, pwrite, const void *buf, size_t count, off_t offset)
        {
            if ((open_flags & O_APPEND) && offset != m_stat.st_size) {
                return callback(done, OPID_PWRITE, (ssize_t)-1, EINVAL);
            }

            do_write(OPID_PWRITE, buf, count, done);
        }
        OVERRIDE_ASYNC(ssize_t, pwritev, const struct iovec *iov, int iovcnt, off_t offset)
        {
            if ((open_flags & O_APPEND) && offset != m_stat.st_size) {
                return callback(done, OPID_PWRITEV, (ssize_t)-1, EINVAL);
            }

            do_writev(OPID_PWRITEV, iov, iovcnt, done);
        }
        OVERRIDE_ASYNC(off_t, lseek, off_t offset, int whence)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC0(int, fsync)
        {
            callback(done, OPID_FSYNC, 0, 0);
        }
        OVERRIDE_ASYNC0(int, fdatasync)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC0(int, close)
        {
            if (upload_id.empty()) {
                if (part_result != 0)
                    return callback(done, OPID_CLOSE, -1, EIO);
                return callback(done, OPID_CLOSE, 0, 0);
            }
            std::thread(
                [this](Done<int> done) {
                    if (part_result != 0 || part_map.empty()) {
                        AbortMultipartUploadRequest aRequest(bucket_name, object_name, upload_id);
                        (void)m_fs->m_client->AbortMultipartUpload(aRequest);
                        upload_id.clear();
                        part_map.clear();
                        if (part_result != 0)
                            return callback(done, OPID_CLOSE, -1, EIO);
                        return callback(done, OPID_CLOSE, 0, 0);
                    }
                    PartList part_list;
                    for (auto& it : part_map) part_list.emplace_back(std::move(it.second));
                    CompleteMultipartUploadRequest request(bucket_name, object_name);
                    request.setUploadId(upload_id);
                    request.setPartList(part_list);
                    auto outcome = m_fs->m_client->CompleteMultipartUpload(request);
                    upload_id.clear();
                    part_map.clear();
                    if (!outcome.isSuccess()) {
                        LOG_ERROR("CompleteMultipartUpload bucket ` object ` fail, ` `", bucket_name, object_name,
                            outcome.error().Code(), outcome.error().Message());
                        return callback(done, OPID_CLOSE, -1, EIO);
                    }
                    return callback(done, OPID_CLOSE, 0, 0);
                },
                done)
                .detach();
        }
        OVERRIDE_ASYNC(int, fchmod, mode_t mode)
        {
            callback_umimplemented(done);
        }
        OVERRIDE_ASYNC(int, fchown, uid_t owner, gid_t group)
        {
            callback_umimplemented(done);
        }
        // OVERRIDE_ASYNC(int, fstat, struct stat *buf)
        // {
        //     callback_umimplemented(done);
        // }
        OVERRIDE_ASYNC(int, ftruncate, off_t length)
        {
            callback_umimplemented(done);
        }
    };
    void ossfs::do_open_stat(string&& bucket_name, string&& object_name,
                             ossfile* file, Done<IAsyncFile*> done,
                             uint64_t timeout) {
        std::thread(
            [this](string&& bucket_name, string&& object_name, ossfile* file,
                   Done<IAsyncFile*> done) {
                //LOG_INFO("do_open_stat ` `", bucket_name, object_name);
                file->m_stat.st_size = 0;
                file->m_stat.st_mtime = 0;
                int ret = stat_for_file(std::string(bucket_name),
                                        std::string(object_name), &file->m_stat);
                if (ret < 0) {
                    if (ret != -ENOENT || !(file->open_flags & O_CREAT)) {
                        delete file;
                        return callback(done, OPID_OPEN, (IAsyncFile*)nullptr,
                                        -ret);
                    }
                } else if ((file->open_flags & O_CREAT) && (file->open_flags & O_EXCL)) {
                    delete file;
                    return callback(done, OPID_OPEN, (IAsyncFile*)nullptr,
                                    EEXIST);
                } else if ((file->open_flags & O_CREAT) && (file->open_flags & O_TRUNC)) {
                    ret = delete_file(std::string(bucket_name), std::string(object_name));
                    if (ret < 0) {
                        delete file;
                        return callback(done, OPID_OPEN, (IAsyncFile*)nullptr,
                                        -ret);
                    }
                    file->m_stat.st_size = 0;
                }
                if (file->open_flags & O_CREAT) {
                    ret = init_file(std::move(bucket_name), std::move(object_name), file->m_stat.st_size, file->open_flags, &file->upload_id, &file->part_number);
                    if (ret < 0) {
                        delete file;
                        return callback(done, OPID_OPEN, (IAsyncFile*)nullptr,
                                        -ret);
                    }
                }
                return callback(done, OPID_OPEN, (IAsyncFile*)file, 0);
            },
            std::move(bucket_name), std::move(object_name), file, done)
            .detach();
    }
    static IAsyncFile* new_oss_file(ossfs* fs, string&& bucket, string&& object, int flags)
    {
        return new ossfile(fs, std::move(bucket), std::move(object), flags);
    }

    class ossdir : public AsyncDIR, public IAsyncBase
    {
    private:
        std::vector<ossitem> m_items;
        long m_loc;

    public:
        ossdir(std::vector<ossitem>&& items) : m_items(std::move(items)), m_loc(0)
        {
        }

        OVERRIDE_ASYNC0(int, closedir)
        {
            callback(done, OPID_CLOSEDIR, 0, 0);
        }

        OVERRIDE_ASYNC0(dirent*, get)
        {
            if (m_loc < 0 || m_loc >= (long)m_items.size())
            {
                return callback(done, OPID_GETDIR, (dirent*)nullptr, EINVAL);
            }

            callback(done, OPID_GETDIR, (dirent*)&m_items[m_loc], 0);
        }

        OVERRIDE_ASYNC0(int, next)
        {
            ++m_loc;
            if (m_loc < 0 || m_loc >= (long)m_items.size())
            {
                return callback(done, OPID_NEXTDIR, 0, EINVAL);
            }

            callback(done, OPID_NEXTDIR, 1, 0);
        }

        OVERRIDE_ASYNC0(void, rewinddir)
        {
            m_loc = 0;
            AsyncResult<void> r;
            r.object = this;
            r.operation = OPID_REWINDDIR;
            r.error_number = 0;
            done(&r);
        }

        OVERRIDE_ASYNC(void, seekdir, long loc)
        {
            m_loc = loc;
            AsyncResult<void> r;
            r.object = this;
            r.operation = OPID_SEEKDIR;
            r.error_number = 0;
            done(&r);
        }

        OVERRIDE_ASYNC0(long, telldir)
        {
            return callback(done, OPID_TELLDIR, m_loc, 0);
        }
    };
    static AsyncDIR* new_oss_dir(std::vector<ossitem>&& items)
    {
        return new ossdir(std::move(items));
    }

    static int translate_log_level(int level) {
        // in osslog means 1~fatal to 5~debug + 6trace and 7all
        // level in alog is 0~debug to 4~fatal
        // alog put all log level >= setting level situation, so all should be 0 or <0 num
        // and none should be >= 5, therefore, (5-level) makes currect transform
        // for alog to oss situation, make -1 means trace and -2 for all,
        // then any numbers greater or equal to 5 means everything off.
        return 5 - level;
    }

    static void oss_alog_adaptor(AlibabaCloud::OSS::LogLevel level, const std::string& log) {
        int alog_level = translate_log_level(level);
        if (alog_level > log_output_level) {
            default_logger.log_output->write(alog_level, log.data(), log.data() + log.length());
        }
    }

    int ossfs_init()
    {
        InitializeSdk();
        AlibabaCloud::OSS::SetLogLevel(AlibabaCloud::OSS::LogOff);
        AlibabaCloud::OSS::SetLogCallback(&oss_alog_adaptor);
        return 0;
    }

    void ossfs_setloglevel(int level) {
        AlibabaCloud::OSS::SetLogLevel((LogLevel)translate_log_level(level));
    }

    IAsyncFileSystem* new_ossfs_from_client(OssClient* client, bool ownership)
    {
        return new ossfs(client, ownership);
    }
    IAsyncFileSystem* new_ossfs(const char* region, const char* bucket,
                                const char* key, const char* key_secret,
                                uint64_t req_timeout, uint64_t conn_timeout,
                                uint64_t maxconn) {
        return new ossfs(region, bucket, key, key_secret, req_timeout,
                         conn_timeout, maxconn);
    }
    int ossfs_fini()
    {
        ShutdownSdk();
        return 0;
    }
}

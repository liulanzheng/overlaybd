#include <memory>
#include <vector>
#include <map>
#include <queue>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "prefetch.h"
#include "overlaybd/fs/forwardfs.h"
#include "overlaybd/fs/localfs.h"
#include "overlaybd/fs/checkedfs/tool/crc32c.h"
#include "overlaybd/alog.h"
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/rpc/serialize.h"
#include "overlaybd/photon/thread11.h"

using namespace std;

namespace FileSystem {

class PrefetcherImpl;

class PrefetchFile : public ForwardFile_Ownership {
public:
    PrefetchFile(IFile* src_file, uint32_t layer_index, Prefetcher* prefetcher);

    ssize_t pread(void* buf, size_t count, off_t offset) override;

private:
    uint32_t m_layer_index;
    PrefetcherImpl* m_prefetcher;
};

class PrefetcherImpl : public Prefetcher {
public:
    PrefetcherImpl(const string& prefetch_dir, const string& trace_file_name) {
        // Create prefetch dir
        string dir_fixed = prefetch_dir;
        if (dir_fixed[dir_fixed.size() - 1] != '/') {
            dir_fixed += '/';
        }
        if (access(dir_fixed.c_str(), F_OK) != 0 && mkdir(dir_fixed.c_str(), 0666) != 0) {
            if (errno != EEXIST) {
                m_mode = Mode::Disabled;
                LOG_ERROR("Prefetch: create dir failed");
                return;
            }
        }
        string full_path = dir_fixed + trace_file_name;

        // Choose mode
        int flags = 0;
        struct stat buf = {};
        int ret = stat(full_path.c_str(), &buf);
        if (ret != 0) {
            m_mode = Mode::Disabled;
        } else if (buf.st_size == 0) {
            m_mode = Mode::Record;
            flags = O_WRONLY;
        } else {
            m_mode = Mode::Replay;
            flags = O_RDONLY;
        }
        m_replay_stopped = false;
        m_record_stopped = false;
        m_buffer_released = false;
        m_lock_file_name = full_path + ".lock";
        m_ok_file_name = full_path + ".ok";
        LOG_INFO("Prefetch: run with mode `, trace file is `", m_mode, full_path);

        // Open trace file
        if (m_mode != Mode::Disabled) {
            m_trace_file = FileSystem::open_localfile_adaptor(full_path.c_str(), flags, 0666, 2);
        } else {
            m_trace_file = nullptr;
        }

        // Loop detect lock file if going to record
        if (m_mode == Mode::Record) {
            int lock_fd = open(m_lock_file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_EXCL, 0666);
            close(lock_fd);
            auto th = photon::thread_create11(&PrefetcherImpl::detect_lock, this);
            m_detect_thread = photon::thread_enable_join(th);
        }

        // Reload if going to replay
        if (m_mode == Mode::Replay) {
            reload(buf.st_size);
        }
    }

    ~PrefetcherImpl() {
        if (m_mode == Mode::Record) {
            m_record_stopped = true;
            if (m_detect_thread_interruptible) {
                photon::thread_shutdown((photon::thread*) m_detect_thread);
            }
            photon::thread_join(m_detect_thread);
            dump();
        } else if (m_mode == Mode::Replay) {
            m_replay_stopped = true;
            for (auto th : m_replay_threads) {
                photon::thread_shutdown((photon::thread*) th);
                photon::thread_join(th);
            }
        }
        if (m_trace_file != nullptr) {
            m_trace_file->close();
            m_trace_file = nullptr;
        }
        LOG_INFO("Prefetcher: object destructed");
    }

    IFile* new_prefetch_file(IFile* src_file, uint32_t layer_index) override {
        return new PrefetchFile(src_file, layer_index, this);
    }

    void record(TraceOp op, uint32_t layer_index, size_t count, off_t offset) override {
        if (m_record_stopped) {
            return;
        }
        TraceFormat trace = {op, layer_index, count, offset};
        m_record_array.push_back(trace);
    }

    void replay() override {
        if (m_mode != Mode::Replay) {
            return;
        }
        if (m_replay_queue.empty() || m_src_files.empty()) {
            return;
        }
        LOG_INFO("Prefetch: Replay ` records from ` layers", m_replay_queue.size(), m_src_files.size());
        for (int i = 0; i < REPLAY_CONCURRENCY; ++i) {
            auto th = photon::thread_create11(&PrefetcherImpl::replay_worker_thread, this);
            auto join_handle = photon::thread_enable_join(th);
            m_replay_threads.push_back(join_handle);
        }
    }

    int replay_worker_thread() {
        static char buf[MAX_IO_SIZE];       // multi threads reuse one buffer
        while (!m_replay_queue.empty() && !m_replay_stopped) {
            auto trace = m_replay_queue.front();
            m_replay_queue.pop();
            auto iter = m_src_files.find(trace.layer_index);
            if (iter == m_src_files.end()) {
                continue;
            }
            auto src_file = iter->second;
            if (trace.op == PrefetcherImpl::TraceOp::READ) {
                ssize_t n_read = src_file->pread(buf, trace.count, trace.offset);
                if (n_read != (ssize_t) trace.count) {
                    LOG_ERROR("Prefetch: replay pread failed: `, `", ERRNO(), trace);
                    continue;
                }
            }
        }
        photon::thread_sleep(3);
        if (!m_buffer_released) {
            m_buffer_released = true;
            madvise(buf, MAX_IO_SIZE, MADV_DONTNEED);
        }
        return 0;
    }

    void register_src_file(uint32_t layer_index, IFile* src_file) {
        m_src_files[layer_index] = src_file;
    }

private:
    struct TraceFormat {
        TraceOp op;
        uint32_t layer_index;
        size_t count;
        off_t offset;
    };

    struct TraceHeader : public RPC::Message {
        uint32_t magic = 0;
        size_t data_size = 0;
        uint32_t checksum = 0;

        PROCESS_FIELDS(data_size, checksum);
    };

    struct TraceContent : public RPC::Message {
        RPC::array<TraceFormat> formats;

        PROCESS_FIELDS(formats);
    };

    static const int MAX_IO_SIZE = 1024 * 1024;
    static const int REPLAY_CONCURRENCY = 16;
    static const uint32_t TRACE_MAGIC = 3270449184; // CRC32 of `Container Image Trace Format`

    vector<TraceFormat> m_record_array;
    queue<TraceFormat> m_replay_queue;
    map<uint32_t, IFile*> m_src_files;
    IFile* m_trace_file;
    string m_lock_file_name;
    string m_ok_file_name;
    vector<photon::join_handle*> m_replay_threads;
    photon::join_handle* m_detect_thread = nullptr;
    bool m_detect_thread_interruptible = false;
    bool m_replay_stopped;
    bool m_record_stopped;
    bool m_buffer_released;

    int dump() {
        if (m_trace_file == nullptr) {
            return 0;
        }

        if (access(m_ok_file_name.c_str(), F_OK) != 0) {
            unlink(m_ok_file_name.c_str());
        }

        auto close_trace_file = [&]() {
            if (m_trace_file != nullptr) {
                m_trace_file->close();
                m_trace_file = nullptr;
            }
        };
        DEFER(close_trace_file());

        TraceContent content;
        content.formats.assign(m_record_array);
        RPC::SerializerIOV serializer_content;
        serializer_content.serialize(content);
        auto size_content = serializer_content.iov.sum();

        TraceHeader hdr;
        hdr.magic = TRACE_MAGIC;
        hdr.checksum = crc32::crc32c(content.formats.begin(), content.formats.length());
        hdr.data_size = size_content;
        RPC::SerializerIOV serializer_hdr;
        serializer_hdr.serialize(hdr);
        auto size_hdr = serializer_hdr.iov.sum();

        ssize_t n_written = m_trace_file->writev(serializer_hdr.iov.iovec(), serializer_hdr.iov.iovcnt());
        if (n_written != (ssize_t) size_hdr) {
            m_trace_file->ftruncate(0);
            LOG_ERRNO_RETURN(0, -1, "Prefetch: dump write header failed");
        }

        n_written = m_trace_file->writev(serializer_content.iov.iovec(), serializer_content.iov.iovcnt());
        if (n_written != (ssize_t) size_content) {
            m_trace_file->ftruncate(0);
            LOG_ERRNO_RETURN(0, -1, "Prefetch: dump write content failed");
        }

        unlink(m_lock_file_name.c_str());
        int ok_fd = open(m_ok_file_name.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_EXCL, 0666);
        if (ok_fd < 0) {
            LOG_ERRNO_RETURN(0, -1, "Prefetch: open OK file failed");
        }
        close(ok_fd);
        return 0;
    }

    int reload(size_t trace_file_size) {
        // Reload header
        IOVector iov_hdr;
        iov_hdr.push_back(sizeof(TraceHeader));
        ssize_t n_read = m_trace_file->readv(iov_hdr.iovec(), iov_hdr.iovcnt());
        if (n_read != sizeof(TraceHeader)) {
            LOG_ERRNO_RETURN(0, -1, "Prefetch: reload header failed");
        }

        RPC::DeserializerIOV deserializer_hdr;
        auto ret_hdr = deserializer_hdr.deserialize<TraceHeader>(&iov_hdr);
        if (TRACE_MAGIC != ret_hdr->magic) {
            LOG_ERROR_RETURN(0, -1, "Prefetch: trace magic mismatch");
        }
        if (trace_file_size != ret_hdr->data_size + sizeof(TraceHeader)) {
            LOG_ERROR_RETURN(0, -1, "Prefetch: trace file size mismatch");
        }

        // Reload content
        IOVector iov_content;
        iov_content.push_back(ret_hdr->data_size);
        n_read = m_trace_file->readv(iov_content.iovec(), iov_content.iovcnt());
        if (n_read != (ssize_t) ret_hdr->data_size) {
            LOG_ERRNO_RETURN(0, -1, "Prefetch: reload content failed");
        }

        RPC::DeserializerIOV deserializer_content;
        auto ret_content = deserializer_content.deserialize<TraceContent>(&iov_content);
        uint32_t checksum = crc32::crc32c(ret_content->formats.begin(), ret_content->formats.length());
        if (checksum != ret_hdr->checksum) {
            LOG_ERROR_RETURN(0, -1, "Prefetch: reload checksum error");
        }

        // Save in memory
        for (auto& each : ret_content->formats) {
            m_replay_queue.push(each);
        }
        LOG_INFO("Prefetch: Reload ` records", m_replay_queue.size());
        return 0;
    }

    int detect_lock() {
        while (!m_record_stopped) {
            m_detect_thread_interruptible = true;
            int ret = photon::thread_sleep(1);
            m_detect_thread_interruptible = false;
            if (ret != 0) {
                break;
            }
            if (access(m_lock_file_name.c_str(), F_OK) != 0) {
                m_record_stopped = true;
                dump();
                break;
            }
        }
        return 0;
    }

    friend LogBuffer& operator<<(LogBuffer& log, const PrefetcherImpl::TraceFormat& f);
};

LogBuffer& operator<<(LogBuffer& log, const PrefetcherImpl::TraceFormat& f) {
    return log << "Op " << char(f.op) << ", Count " << f.count << ", Offset " << f.offset << ", Layer_index "
               << f.layer_index;
}

PrefetchFile::PrefetchFile(IFile* src_file, uint32_t layer_index, Prefetcher* prefetcher) :
    ForwardFile_Ownership(src_file, true),
    m_layer_index(layer_index),
    m_prefetcher((PrefetcherImpl*) prefetcher) {
    if (m_prefetcher->get_mode() == PrefetcherImpl::Mode::Replay) {
        m_prefetcher->register_src_file(layer_index, src_file);
    }
}

ssize_t PrefetchFile::pread(void* buf, size_t count, off_t offset) {
    if (m_prefetcher->get_mode() == PrefetcherImpl::Mode::Record) {
        m_prefetcher->record(PrefetcherImpl::TraceOp::READ, m_layer_index, count, offset);
    }
    return m_file->pread(buf, count, offset);
}

Prefetcher* new_prefetcher(const string& prefetch_dir, const string& trace_file_name) {
    return new PrefetcherImpl(prefetch_dir, trace_file_name);
}

}

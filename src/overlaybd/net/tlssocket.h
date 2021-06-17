#pragma once
#include "socket.h"

struct ssl_st;
typedef struct ssl_st SSL;

namespace Net {

int ssl_init(const char* cert_path, const char* key_path,
             const char* passphrase);
int ssl_init(void* cert, void* key);
int ssl_init();
void* ssl_get_ctx();
int ssl_set_cert(const char* cert);
int ssl_set_pkey(const char* key, const char* passphrase);
int ssl_fini();

ssize_t ssl_read(SSL* ssl, void* buf, size_t count, uint64_t timeout);
ssize_t ssl_write(SSL* ssl, const void* buf, size_t count, uint64_t timeout);
ssize_t ssl_read_n(SSL* ssl, void* buf, size_t count, uint64_t timeout);
ssize_t ssl_write_n(SSL* ssl, const void* buf, size_t count, uint64_t timeout);
ssize_t ssl_connect(SSL* ssl, uint64_t timeout);
ssize_t ssl_accept(SSL* ssl, uint64_t timeout);
ssize_t set_cert(const char* path);
ssize_t set_key(const char* path);

ISocketClient* new_tls_socket_client();

}  // namespace Net

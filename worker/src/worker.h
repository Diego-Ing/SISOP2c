#ifndef WORKER_H
#define WORKER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <stdbool.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <../../utils/src/utils/conexiones.h>
#include <../../utils/src/utils/protocolos.h>
#include <pthread.h>


// ====== Globals comunes ======
extern t_log*   g_wlogger;
extern int      g_fd_master;
extern int      g_fd_storage;
extern uint32_t g_block_size;      // handshake Storage
extern uint32_t g_worker_id;

// Config del Worker
extern size_t   g_mem_size_bytes;  // TAM_MEMORIA
extern uint32_t g_mem_delay_ms;    // RETARDO_MEMORIA
typedef enum { REEMPLAZO_LRU, REEMPLAZO_CLOCKM } t_reemplazo_algo;
extern t_reemplazo_algo g_reemplazo;   // LRU / CLOCK-M
extern char*    g_path_scripts;        // PATH_SCRIPTS (malloc)

// ====== Master listener / Exec ======
void* master_listener_thread(void* _);
void  worker_exec_init(void);
void  worker_exec_shutdown(void);
void  worker_exec_start(uint32_t qid, uint32_t pc_inicial, const char* path_query);
void  worker_exec_request_preempt(uint32_t qid);

// ====== Storage API ======
int    storage_connect_and_handshake(const char* ip, const char* puerto);
int    storage_create(const char* file, const char* tag);
int    storage_truncate(const char* file, const char* tag, uint32_t new_size);
int    storage_delete(const char* file, const char* tag);
int    storage_commit(const char* file, const char* tag);
int    storage_tag(const char* f_src, const char* t_src, const char* f_dst, const char* t_dst);
// bloques
char*  storage_get_block(const char* file, const char* tag, uint32_t page); // malloc de size=BLOCK_SIZE
int    storage_put_block(const char* file, const char* tag, uint32_t page, const char* data, uint32_t len);

// ====== Memoria Interna ======
void   mem_init(size_t mem_bytes, uint32_t page_size, t_reemplazo_algo algo, uint32_t delay_ms);
void   mem_destroy(void);
char*  mem_read(uint32_t qid, const char* file, const char* tag, size_t base, size_t size); // malloc con size bytes
int    mem_write(uint32_t qid, const char* file, const char* tag, size_t base, const char* data, size_t len);
void   mem_flush_file(uint32_t qid, const char* file, const char* tag);
void   mem_invalidate_from_page(uint32_t qid, const char* file, const char* tag, uint32_t first_page);
void   mem_flush_set(uint32_t qid, t_list* touched_filetags);  // elementos "file:tag"
void   mem_drop_file(const char* file, const char* tag);       // liberar frames de ese file:tag

#endif

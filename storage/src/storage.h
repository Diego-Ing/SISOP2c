#ifndef STORAGE_H
#define STORAGE_H

#include <stdint.h>
#include <stdbool.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/bitarray.h>
#include <commons/collections/list.h>
#include <commons/collections/dictionary.h>
#include <commons/string.h>
#include <commons/crypto.h>   // crypto_md5
#include <sys/stat.h>
#include <pthread.h>
#include <unistd.h>

#include <../../utils/src/utils/conexiones.h>
#include <../../utils/src/utils/protocolos.h>

// ====== Config ======
typedef struct {
    char* puerto_escucha;        // PUERTO_ESCUCHA
    bool  fresh_start;           // FRESH_START
    char* root;                  // PUNTO_MONTAJE
    uint32_t ret_op_ms;          // RETARDO_OPERACION
    uint32_t ret_blk_ms;         // RETARDO_ACCESO_BLOQUE
    t_log_level log_level;       // LOG_LEVEL (string->level)
} t_st_cfg;

extern t_st_cfg g_cfg;
extern t_log*   g_logger;
extern int      g_server_fd;
extern uint32_t g_fs_size;
extern uint32_t g_block_size;
extern uint32_t g_blocks_count;

// ====== FS paths ======
char* path_superblock(void);
char* path_bitmap(void);
char* path_hashindex(void);
char* path_physical_dir(void);
char* path_block_n(uint32_t n);
char* path_files_dir(void);
char* path_file_dir(const char* file);
char* path_tag_dir(const char* file, const char* tag);
char* path_tag_metadata(const char* file, const char* tag);
char* path_tag_logical_dir(const char* file, const char* tag);
char* path_tag_logical_block(const char* file, const char* tag, uint32_t logical);

// ====== FS mount / format ======
bool  storage_load_config(const char* cfg_path);
void  storage_free_config(void);
bool  fs_mount_or_format(void);     // monta o formatea según FRESH_START
void  fs_unmount(void);
bool  ensure_dir(const char* path);

// ====== Bitmap / hash index ======
extern t_bitarray* g_bitmap;
extern int         g_bitmap_fd;
extern pthread_mutex_t m_bitmap;

bool  bm_is_set(uint32_t blk);
void  bm_set(uint32_t blk);
void  bm_clear(uint32_t blk);
int   bm_find_free(void);           // -1 si no hay

extern t_config* g_hash_index_cfg;  // blocks_hash_index.config
extern pthread_mutex_t m_hashidx;
char* hi_get_block_by_md5(const char* md5hex); // strdup del nombre (blockNNNN.dat) o NULL
void  hi_put_md5(const char* md5hex, const char* block_name);
void  hi_sync(void);

// ====== Tag metadata ======
typedef struct {
    uint32_t size;            // TAMAÑO
    char*    estado;          // "WORK_IN_PROGRESS" | "COMMITED"
    t_list*  blocks;          // lista de uint32* (números de bloque físico)
} t_tagmeta;

t_tagmeta* meta_load(const char* file, const char* tag);
bool       meta_save(const char* file, const char* tag, t_tagmeta* m);
void       meta_destroy(t_tagmeta* m);

// ====== Helpers ======
void    delay_op(void);
void    delay_block(void);
uint32_t physical_refcount(uint32_t blk); // por nlink de block file
bool    ensure_hardlink(const char* logical_path, const char* physical_block_path);
bool    replace_hardlink(const char* logical_path, const char* new_physical_block_path);
bool    remove_logical_link(const char* logical_path);

#define STATUS_OK                0u
#define ERR_FILE_INEXISTENTE     1u
#define ERR_TAG_INEXISTENTE      2u
#define ERR_SIN_ESPACIO          3u
#define ERR_NO_PERMITIDO         4u
#define ERR_FUERA_DE_LIMITE      5u
#define ERR_IO                   6u

// ====== Server / Protocolo ======
void* accept_loop(void* _);
void  handle_worker(int fd);
// expone lista y mutex para contarlos en el protocolo
extern t_list* g_workers;
extern pthread_mutex_t m_workers;

// ====== Operaciones ======
uint32_t op_handshake_blocksize(int fd);
uint32_t op_create(uint32_t qid, const char* file, const char* tag);
uint32_t op_truncate(uint32_t qid, const char* file, const char* tag, uint32_t new_size);
uint32_t op_tag(uint32_t qid, const char* fsrc, const char* tsrc, const char* fdst, const char* tdst);
uint32_t op_commit(uint32_t qid, const char* file, const char* tag);
uint32_t op_delete(uint32_t qid, const char* file, const char* tag);
uint32_t op_get_block(uint32_t qid, const char* file, const char* tag, uint32_t logical, char** out_data); // malloc BLOCK_SIZE
uint32_t op_put_block(uint32_t qid, const char* file, const char* tag, uint32_t logical, const char* data, uint32_t len);

// ====== Logs requeridos ======
void log_worker_conectado(uint32_t wid, int cant);
void log_worker_desconectado(uint32_t wid, int cant);
void log_file_creado(uint32_t qid, const char* file, const char* tag);
void log_file_truncado(uint32_t qid, const char* file, const char* tag, uint32_t tam);
void log_tag_creado(uint32_t qid, const char* file, const char* tag);
void log_commit(uint32_t qid, const char* file, const char* tag);
void log_tag_eliminado(uint32_t qid, const char* file, const char* tag);
void log_bloque_leido(uint32_t qid, const char* file, const char* tag, uint32_t logical);
void log_bloque_escrito(uint32_t qid, const char* file, const char* tag, uint32_t logical);
void log_bf_reservado(uint32_t qid, uint32_t blk);
void log_bf_liberado(uint32_t qid, uint32_t blk);
void log_hl_agregado(uint32_t qid, const char* file, const char* tag, uint32_t logi, uint32_t phys);
void log_hl_eliminado(uint32_t qid, const char* file, const char* tag, uint32_t logi, uint32_t phys);
void log_dedupe(uint32_t qid, const char* file, const char* tag, uint32_t logical, uint32_t from_phys, uint32_t to_phys);

#endif

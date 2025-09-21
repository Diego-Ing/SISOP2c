#include "storage.h"
#include <dirent.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>   // mmap, msync, PROT_*, MAP_*, MS_SYNC
#include <signal.h>     // signal
#include <errno.h>      // errno / EEXIST

t_st_cfg g_cfg;
t_log*   g_logger = NULL;
int      g_server_fd = -1;
uint32_t g_fs_size = 0;
uint32_t g_block_size = 0;
uint32_t g_blocks_count = 0;

t_bitarray* g_bitmap = NULL;
int g_bitmap_fd = -1;
pthread_mutex_t m_bitmap = PTHREAD_MUTEX_INITIALIZER;

t_config* g_hash_index_cfg = NULL;
pthread_mutex_t m_hashidx = PTHREAD_MUTEX_INITIALIZER;

// ===== Config =====
static t_log_level level_from(const char* s){
    t_log_level l = log_level_from_string((char*)s);
    return l == (t_log_level)-1 ? LOG_LEVEL_INFO : l;
}

bool storage_load_config(const char* cfg_path){
    t_config* c = config_create((char*)cfg_path);  // commons espera char*

    if(!c) return false;
    g_cfg.puerto_escucha = strdup(config_get_string_value(c, "PUERTO_ESCUCHA"));
    const char* fs = config_get_string_value(c, "FRESH_START");
    g_cfg.fresh_start = (fs && (!strcasecmp(fs,"true") || !strcmp(fs,"1")));
    g_cfg.root           = strdup(config_get_string_value(c, "PUNTO_MONTAJE"));
    g_cfg.ret_op_ms      = (uint32_t)config_get_int_value(c, "RETARDO_OPERACION");
    g_cfg.ret_blk_ms     = (uint32_t)config_get_int_value(c, "RETARDO_ACCESO_BLOQUE");
    g_cfg.log_level      = level_from(config_get_string_value(c, "LOG_LEVEL"));
    g_logger = log_create("storage.log", "STORAGE", 1, g_cfg.log_level);
    config_destroy(c);
    return g_logger != NULL;
}
void storage_free_config(void){
    if(g_bitmap) { bitarray_destroy(g_bitmap); g_bitmap=NULL; }
    if(g_bitmap_fd!=-1){ close(g_bitmap_fd); g_bitmap_fd=-1; }
    if(g_hash_index_cfg){ config_save(g_hash_index_cfg); config_destroy(g_hash_index_cfg); g_hash_index_cfg=NULL; }
    if(g_server_fd!=-1){ close(g_server_fd); g_server_fd=-1; }
    if(g_logger){ log_destroy(g_logger); g_logger=NULL; }
    free(g_cfg.puerto_escucha); free(g_cfg.root);
}
static t_config* config_create_empty_file(const char* path) {
    FILE* f = fopen(path, "w");  // crea vacío si no existe
    if(f) fclose(f);
    return config_create((char*)path);
}

// ===== Paths =====
char* path_superblock(void){ return string_from_format("%s/superblock.config", g_cfg.root); }
char* path_bitmap(void){ return string_from_format("%s/bitmap.bin", g_cfg.root); }
char* path_hashindex(void){ return string_from_format("%s/blocks_hash_index.config", g_cfg.root); }
char* path_physical_dir(void){ return string_from_format("%s/physical_blocks", g_cfg.root); }
char* path_block_n(uint32_t n){ char* dir=path_physical_dir(); char* p=string_from_format("%s/block%04u.dat", dir, n); free(dir); return p; }
char* path_files_dir(void){ return string_from_format("%s/files", g_cfg.root); }
char* path_file_dir(const char* file){ char* f=path_files_dir(); char* p=string_from_format("%s/%s", f, file); free(f); return p; }
char* path_tag_dir(const char* file, const char* tag){ char* fd=path_file_dir(file); char* p=string_from_format("%s/%s", fd, tag); free(fd); return p; }
char* path_tag_metadata(const char* file, const char* tag){ char* td=path_tag_dir(file,tag); char* p=string_from_format("%s/metadata.config", td); free(td); return p; }
char* path_tag_logical_dir(const char* file, const char* tag){ char* td=path_tag_dir(file,tag); char* p=string_from_format("%s/logical_blocks", td); free(td); return p; }
char* path_tag_logical_block(const char* file, const char* tag, uint32_t logical){
    char* ld=path_tag_logical_dir(file,tag); char* p=string_from_format("%s/%06u.dat", ld, logical); free(ld); return p;
}

// ===== Delays =====
void delay_op(void){ usleep(g_cfg.ret_op_ms * 1000); }
void delay_block(void){ usleep(g_cfg.ret_blk_ms * 1000); }

// ===== Bitmap helpers =====
bool bm_is_set(uint32_t blk){ pthread_mutex_lock(&m_bitmap); bool v=bitarray_test_bit(g_bitmap, blk); pthread_mutex_unlock(&m_bitmap); return v; }

void bm_set(uint32_t blk){
    pthread_mutex_lock(&m_bitmap);
    bitarray_set_bit(g_bitmap, blk);
    msync(g_bitmap->bitarray, g_bitmap->size, MS_SYNC);
    pthread_mutex_unlock(&m_bitmap);
}
void bm_clear(uint32_t blk){
    pthread_mutex_lock(&m_bitmap);
    bitarray_clean_bit(g_bitmap, blk);
    msync(g_bitmap->bitarray, g_bitmap->size, MS_SYNC);
    pthread_mutex_unlock(&m_bitmap);
}

int bm_find_free(void){
    pthread_mutex_lock(&m_bitmap);
    for(uint32_t i=0;i<g_blocks_count;i++) if(!bitarray_test_bit(g_bitmap,i)){ pthread_mutex_unlock(&m_bitmap); return (int)i; }
    pthread_mutex_unlock(&m_bitmap);
    return -1;
}

// ===== Hash index =====
char* hi_get_block_by_md5(const char* md5hex){
    pthread_mutex_lock(&m_hashidx);
    if(!config_has_property(g_hash_index_cfg, (char*)md5hex)){ pthread_mutex_unlock(&m_hashidx); return NULL; }
    char* v = strdup(config_get_string_value(g_hash_index_cfg, (char*)md5hex));
    pthread_mutex_unlock(&m_hashidx);
    return v;
}
void hi_put_md5(const char* md5hex, const char* block_name){
    pthread_mutex_lock(&m_hashidx);
    config_set_value(g_hash_index_cfg, (char*)md5hex, (char*)block_name);
    pthread_mutex_unlock(&m_hashidx);
}
void hi_sync(void){
    pthread_mutex_lock(&m_hashidx);
    config_save(g_hash_index_cfg);
    pthread_mutex_unlock(&m_hashidx);
}

// ===== Meta =====
t_tagmeta* meta_load(const char* file, const char* tag){
    char* mp = path_tag_metadata(file,tag);
    if(access(mp,F_OK)!=0){ free(mp); return NULL; }
    t_config* c = config_create(mp); free(mp); if(!c) return NULL;
    t_tagmeta* m = calloc(1,sizeof(*m));
    m->size = (uint32_t)config_get_int_value(c,"TAMAÑO");
    m->estado = strdup(config_get_string_value(c,"ESTADO"));
    m->blocks = list_create();
    char** arr = config_get_array_value(c,"BLOCKS");
    for(int i=0; arr && arr[i]; ++i){ uint32_t* v=malloc(sizeof(uint32_t)); *v=(uint32_t)strtoul(arr[i],NULL,10); list_add(m->blocks,v); }
    if(arr) free(arr);
    config_destroy(c);
    return m;
}
bool meta_save(const char* file, const char* tag, t_tagmeta* m){
    char* mp = path_tag_metadata(file,tag);
    t_config* c = config_create(mp);
    if(!c){ // crear nuevo
        c = config_create_empty_file(mp);
        config_set_value(c,"TAMAÑO","0");
        config_set_value(c,"BLOCKS","[]");
        config_set_value(c,"ESTADO","WORK_IN_PROGRESS");
    }
    char* sz = string_from_format("%u", m->size);
    config_set_value(c, "TAMAÑO", sz); free(sz);
    // blocks -> [a,b,c]
    char* bl = string_new();
    string_append(&bl,"[");
    for(int i=0;i<list_size(m->blocks);++i){
        uint32_t* v=list_get(m->blocks,i);
        char* s=string_from_format("%u", *v);
        string_append(&bl, s);
        free(s);
        if(i+1<list_size(m->blocks)) string_append(&bl,",");
    }
    string_append(&bl,"]");
    config_set_value(c,"BLOCKS", bl); free(bl);
    config_set_value(c,"ESTADO", m->estado);
    bool ok = config_save_in_file(c, mp);
    config_destroy(c); free(mp);
    return ok;
}
void meta_destroy(t_tagmeta* m){
    if(!m) return;
    for(int i=0;i<list_size(m->blocks);++i) free(list_get(m->blocks,i));
    list_destroy(m->blocks); free(m->estado); free(m);
}

// ===== mount/format =====
bool ensure_dir(const char* p){ return (mkdir(p, 0755)==0) || errno==EEXIST; }

// rm -rf util para FRESH_START
static void rm_rf(const char* path) {
    DIR* d = opendir(path);
    if(!d){ unlink(path); return; }
    struct dirent* e;
    while((e = readdir(d))){
        if(!strcmp(e->d_name,".") || !strcmp(e->d_name,"..")) continue;
        char* p = string_from_format("%s/%s", path, e->d_name);
        rm_rf(p);
        free(p);
    }
    closedir(d);
    rmdir(path);
}

static bool create_physical_space(uint32_t blocks, uint32_t block_size){
    char* pd = path_physical_dir(); if(!ensure_dir(pd)){ free(pd); return false; }
    for(uint32_t i=0;i<blocks;i++){
        char* bf = path_block_n(i);
        int fd = open(bf, O_CREAT|O_RDWR, 0644);
        if(fd<0){ free(bf); free(pd); return false; }
        // asegurar tamaño
        if(ftruncate(fd, block_size)!=0){ close(fd); free(bf); free(pd); return false; }
        close(fd); free(bf);
    }
    free(pd); return true;
}
static bool create_bitmap(uint32_t blocks){
    char* bp = path_bitmap();
    g_bitmap_fd = open(bp, O_CREAT|O_RDWR, 0644);
    size_t bytes = (blocks + 7)/8;
    if(ftruncate(g_bitmap_fd, bytes)!=0){ free(bp); return false; }
    void* map = mmap(NULL, bytes, PROT_READ|PROT_WRITE, MAP_SHARED, g_bitmap_fd, 0);
    if(map==MAP_FAILED){ free(bp); return false; }
    g_bitmap = bitarray_create_with_mode(map, bytes, LSB_FIRST);
    // inicia todo en 0
    for(uint32_t i=0;i<blocks;i++) bitarray_clean_bit(g_bitmap,i);
    msync(g_bitmap->bitarray, g_bitmap->size, MS_SYNC);
    free(bp); return true;
}
static bool open_bitmap(uint32_t blocks){
    char* bp = path_bitmap();
    g_bitmap_fd = open(bp, O_RDWR); size_t bytes=(blocks+7)/8;
    void* map = mmap(NULL, bytes, PROT_READ|PROT_WRITE, MAP_SHARED, g_bitmap_fd, 0);
    if(map==MAP_FAILED){ free(bp); return false; }
    g_bitmap = bitarray_create_with_mode(map, bytes, LSB_FIRST);
    free(bp); return true;
}
static bool create_hashindex(void){
    char* hp = path_hashindex();
    g_hash_index_cfg = config_create(hp);
    if(!g_hash_index_cfg) g_hash_index_cfg = config_create_empty_file(hp);
    bool ok = config_save_in_file(g_hash_index_cfg, hp);
    free(hp); return ok;
}
static bool open_hashindex(void){
    char* hp = path_hashindex();
    g_hash_index_cfg = config_create(hp);
    if(!g_hash_index_cfg) g_hash_index_cfg = config_create_empty_file(hp); // tolerante
    free(hp); return true;
}

static bool create_initial_file(void){
    // marca bloque 0 ocupado y llena con '0'
    bm_set(0);
    char* b0 = path_block_n(0);
    int fd = open(b0, O_RDWR); if(fd<0){ free(b0); return false; }
    char* zeros = malloc(g_block_size); memset(zeros,'0',g_block_size); write(fd, zeros, g_block_size);
    close(fd); free(zeros);

    // /files/initial_file/BASE/{metadata,logical_blocks/000000.dat -> block0000.dat}
    char* files_dir = path_files_dir(); if(!ensure_dir(files_dir)){ free(files_dir); free(b0); return false; }
    char* fdir = path_file_dir("initial_file"); if(!ensure_dir(fdir)){ free(files_dir); free(fdir); free(b0); return false; }
    char* tdir = path_tag_dir("initial_file","BASE"); if(!ensure_dir(tdir)){ free(files_dir); free(fdir); free(tdir); free(b0); return false; }
    char* ldir = path_tag_logical_dir("initial_file","BASE"); if(!ensure_dir(ldir)){ free(files_dir); free(fdir); free(tdir); free(ldir); free(b0); return false; }

    // metadata
    t_tagmeta m = { .size = g_block_size, .estado = "COMMITED", .blocks = list_create() };
    uint32_t* z = malloc(sizeof(uint32_t)); *z = 0; list_add(m.blocks, z);
    meta_save("initial_file","BASE",&m);
    for(int i=0;i<list_size(m.blocks);++i) free(list_get(m.blocks,i));
    list_destroy(m.blocks);

    // logical link
    char* lb0 = path_tag_logical_block("initial_file","BASE",0);
    link(b0, lb0);
    free(lb0); free(ldir); free(tdir); free(fdir); free(files_dir); free(b0);
    return true;
}

bool fs_mount_or_format(void){
    // leer superblock
    char* sp = path_superblock();
    t_config* sb = config_create(sp);
    if(!sb && g_cfg.fresh_start){
        log_info(g_logger,"No hay superblock. Creá %s con FS_SIZE y BLOCK_SIZE antes de iniciar.", sp);
        free(sp);
        return false;
    }
    if(!sb){ free(sp); return false; }
    g_fs_size   = (uint32_t)config_get_int_value(sb, "FS_SIZE");
    g_block_size= (uint32_t)config_get_int_value(sb, "BLOCK_SIZE");
    g_blocks_count = g_fs_size / g_block_size;
    config_destroy(sb); free(sp);

    // crear arbol base
    char* root = g_cfg.root; mkdir(root, 0755);

    if(g_cfg.fresh_start){
        // wipe completo
        char* phys = path_physical_dir(); rm_rf(phys); free(phys);
        char* files = path_files_dir();   rm_rf(files); free(files);
        char* bm = path_bitmap();         unlink(bm);   free(bm);
        char* hi = path_hashindex();      unlink(hi);   free(hi);

        // recrear estructura
        phys = path_physical_dir(); ensure_dir(phys); free(phys);
        files = path_files_dir();   ensure_dir(files); free(files);

        create_physical_space(g_blocks_count, g_block_size);
        create_bitmap(g_blocks_count);
        create_hashindex();
        create_initial_file();
    } else {
        char* phys = path_physical_dir(); ensure_dir(phys); free(phys);
        char* files = path_files_dir(); ensure_dir(files); free(files);

        open_bitmap(g_blocks_count);
        open_hashindex();
    }
    return true;
}
void fs_unmount(void){ hi_sync(); }

static void sigint_handler(int _){ (void)_; fs_unmount(); storage_free_config(); _exit(0); }

// ===== Logs requeridos =====
void log_worker_conectado(uint32_t wid, int cant){
    log_info(g_logger, "##Se conecta el Worker %u - Cantidad de Workers: %d", wid, cant);
}
void log_worker_desconectado(uint32_t wid, int cant){
    log_info(g_logger, "##Se desconecta el Worker %u - Cantidad de Workers: %d", wid, cant);
}
void log_file_creado(uint32_t qid, const char* file, const char* tag){
    log_info(g_logger, "##%u - File Creado %s:%s", qid, file, tag);
}
void log_file_truncado(uint32_t qid, const char* file, const char* tag, uint32_t tam){
    log_info(g_logger, "##%u - File Truncado %s:%s - Tamaño: %u", qid, file, tag, tam);
}
void log_tag_creado(uint32_t qid, const char* file, const char* tag){
    log_info(g_logger, "##%u - Tag creado %s:%s", qid, file, tag);
}
void log_commit(uint32_t qid, const char* file, const char* tag){
    log_info(g_logger, "##%u - Commit de File:Tag %s:%s", qid, file, tag);
}
void log_tag_eliminado(uint32_t qid, const char* file, const char* tag){
    log_info(g_logger, "##%u - Tag Eliminado %s:%s", qid, file, tag);
}
void log_bloque_leido(uint32_t qid, const char* file, const char* tag, uint32_t logical){
    log_info(g_logger, "##%u - Bloque Lógico Leído %s:%s - Número de Bloque: %u", qid, file, tag, logical);
}
void log_bloque_escrito(uint32_t qid, const char* file, const char* tag, uint32_t logical){
    log_info(g_logger, "##%u - Bloque Lógico Escrito %s:%s - Número de Bloque: %u", qid, file, tag, logical);
}
void log_bf_reservado(uint32_t qid, uint32_t blk){
    log_info(g_logger, "##%u - Bloque Físico Reservado - Número de Bloque: %u", qid, blk);
}
void log_bf_liberado(uint32_t qid, uint32_t blk){
    log_info(g_logger, "##%u - Bloque Físico Liberado - Número de Bloque: %u", qid, blk);
}
void log_hl_agregado(uint32_t qid, const char* file, const char* tag, uint32_t logi, uint32_t phys){
    log_info(g_logger, "##%u - %s:%s Se agregó el hard link del bloque lógico %u al bloque físico %u", qid, file, tag, logi, phys);
}
void log_hl_eliminado(uint32_t qid, const char* file, const char* tag, uint32_t logi, uint32_t phys){
    log_info(g_logger, "##%u - %s:%s Se eliminó el hard link del bloque lógico %u al bloque físico %u", qid, file, tag, logi, phys);
}
void log_dedupe(uint32_t qid, const char* file, const char* tag, uint32_t logical, uint32_t from_phys, uint32_t to_phys){
    log_info(g_logger, "##%u - %s:%s Bloque Lógico %u se reasigna de %u a %u", qid, file, tag, logical, from_phys, to_phys);
}

// ===== Helpers FS =====
uint32_t physical_refcount(uint32_t blk){
    char* p = path_block_n(blk);
    struct stat st; uint32_t rc=0;
    if(stat(p,&st)==0) rc = (uint32_t)st.st_nlink; // incluye el propio archivo físico
    free(p);
    if(rc==0) return 0;
    if(rc==1) return 0; // sólo el archivo físico, sin lógicos
    return rc-1;        // cantidad de hardlinks lógicos
}
bool ensure_hardlink(const char* logical_path, const char* physical_block_path){
    if(access(logical_path,F_OK)==0) return true;         // ya existe
    return link(physical_block_path, logical_path)==0;    // crear HL
}
bool replace_hardlink(const char* logical_path, const char* new_physical_block_path){
    unlink(logical_path);                                  // si no existe, no pasa nada
    return link(new_physical_block_path, logical_path) == 0;
}
bool remove_logical_link(const char* logical_path){
    if(access(logical_path, F_OK) != 0) return true;       // ya no existe
    return unlink(logical_path) == 0;
}

// ===== Accept loop =====
t_list* g_workers = NULL;
pthread_mutex_t m_workers = PTHREAD_MUTEX_INITIALIZER;

void* accept_loop(void* _){
    (void)_;
    g_workers = list_create();
    for(;;){
        int fd = esperar_cliente(g_server_fd);
        if(fd<0) continue;

        // Handshake esperado:
        int op = recibir_operacion(fd);
        if(op != STORAGE_HANDSHAKE){ if(op>0){ t_paquete* throw=recibir_paquete(fd); if(throw) eliminar_paquete(throw);} close(fd); continue; }

        // responder BLOCK_SIZE
        t_paquete* resp = crear_paquete(STORAGE_BLOCK_SIZE);
        agregar_a_paquete(resp, &g_block_size, sizeof(uint32_t));
        enviar_paquete(resp, fd); eliminar_paquete(resp);

        pthread_mutex_lock(&m_workers); list_add(g_workers, (void*)(intptr_t)fd); int cant=list_size(g_workers); pthread_mutex_unlock(&m_workers);
        log_worker_conectado(0, cant); // no tenemos WORKER_ID en el protocolo → 0

        pthread_t th; pthread_create(&th,NULL,(void*(*)(void*))handle_worker,(void*)(intptr_t)fd);
        pthread_detach(th);
    }
    return NULL;
}

int main(int argc, char** argv){
    if(argc<2){ fprintf(stderr,"Uso: %s storage.config\n", argv[0]); return 1; }
    if(!storage_load_config(argv[1])){ fprintf(stderr,"No pude cargar config\n"); return 1; }
    signal(SIGINT, sigint_handler);

    if(!fs_mount_or_format()){ fprintf(stderr,"No pude montar/formatear FS\n"); return 1; }

    g_server_fd = iniciar_servidor(g_cfg.puerto_escucha);
    if(g_server_fd<0){ log_error(g_logger,"No pude iniciar servidor en %s", g_cfg.puerto_escucha); return 1; }
    pthread_t th; pthread_create(&th,NULL,accept_loop,NULL); pthread_detach(th);

    for(;;) pause();
    return 0;
}

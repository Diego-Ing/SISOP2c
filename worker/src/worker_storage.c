// worker_storage.c

#include "worker.h"

// utils
static inline void add_cstring(t_paquete* p, const char* s){
    int len = s ? (int)strlen(s)+1 : 1;
    agregar_a_paquete(p, &len, sizeof(int));
    agregar_a_paquete(p, (void*)s, len);
}
static uint32_t read_u32_from_pkg(t_paquete* p){ uint32_t v=0; buffer_read(&v,p->buffer,sizeof(uint32_t)); return v; }

int storage_connect_and_handshake(const char* ip, const char* puerto){
    g_fd_storage = crear_conexion((char*)ip, (char*)puerto);
    if(g_fd_storage < 0) return -1;

    t_paquete* hello = crear_paquete(STORAGE_HANDSHAKE);
    enviar_paquete(hello, g_fd_storage); eliminar_paquete(hello);

    int op = recibir_operacion(g_fd_storage);
    if(op != STORAGE_BLOCK_SIZE){ t_paquete* d=recibir_paquete(g_fd_storage); if(d) eliminar_paquete(d); return -1; }
    t_paquete* resp = recibir_paquete(g_fd_storage); resp->buffer->offset = 0;
    buffer_read(&g_block_size, resp->buffer, sizeof(uint32_t)); eliminar_paquete(resp);

    log_info(g_wlogger,"Storage conectado. BLOCK_SIZE=%u", g_block_size);
    return g_fd_storage;
}

int storage_create(const char* file, const char* tag){
    t_paquete* req = crear_paquete(STORAGE_CREATE); add_cstring(req,file); add_cstring(req,tag);
    enviar_paquete(req,g_fd_storage); eliminar_paquete(req);
    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_CREATE){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}
int storage_truncate(const char* file, const char* tag, uint32_t new_size){
    t_paquete* req = crear_paquete(STORAGE_TRUNCATE); add_cstring(req,file); add_cstring(req,tag);
    agregar_a_paquete(req,&new_size,sizeof(uint32_t)); enviar_paquete(req,g_fd_storage); eliminar_paquete(req);
    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_TRUNCATE){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}
int storage_delete(const char* file, const char* tag){
    t_paquete* req = crear_paquete(STORAGE_DELETE); add_cstring(req,file); add_cstring(req,tag);
    enviar_paquete(req,g_fd_storage); eliminar_paquete(req);
    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_DELETE){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}
int storage_commit(const char* file, const char* tag){
    t_paquete* req = crear_paquete(STORAGE_COMMIT); add_cstring(req,file); add_cstring(req,tag);
    enviar_paquete(req,g_fd_storage); eliminar_paquete(req);
    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_COMMIT){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}
int storage_tag(const char* fsrc, const char* tsrc, const char* fdst, const char* tdst){
    t_paquete* req = crear_paquete(STORAGE_TAG);
    add_cstring(req,fsrc); add_cstring(req,tsrc); add_cstring(req,fdst); add_cstring(req,tdst);
    enviar_paquete(req,g_fd_storage); eliminar_paquete(req);
    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_TAG){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}

// bloques
char* storage_get_block(const char* file, const char* tag, uint32_t page){
    t_paquete* req=crear_paquete(STORAGE_GET_BLOCK); add_cstring(req,file); add_cstring(req,tag);
    agregar_a_paquete(req,&page,sizeof(uint32_t)); enviar_paquete(req,g_fd_storage); eliminar_paquete(req);

    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_GET_BLOCK){ if(r) eliminar_paquete(r); return NULL; }
    r->buffer->offset=0;
    // asumimos el Storage devuelve exactamente BLOCK_SIZE bytes
    char* data = calloc(g_block_size,1);
    memcpy(data, r->buffer->stream + r->buffer->offset, g_block_size);
    eliminar_paquete(r);
    return data; // malloc BLOCK_SIZE
}
int storage_put_block(const char* file, const char* tag, uint32_t page, const char* data, uint32_t len){
    t_paquete* req=crear_paquete(STORAGE_PUT_BLOCK); add_cstring(req,file); add_cstring(req,tag);
    agregar_a_paquete(req,&page,sizeof(uint32_t));
    // enviamos len seguido de bytes (para no forzar BLOCK_SIZE exacto)
    agregar_a_paquete(req,&len,sizeof(uint32_t));
    agregar_a_paquete(req,(void*)data,len);
    enviar_paquete(req,g_fd_storage); eliminar_paquete(req);

    int op=recibir_operacion(g_fd_storage); t_paquete* r=recibir_paquete(g_fd_storage);
    if(op!=STORAGE_PUT_BLOCK){ if(r) eliminar_paquete(r); return -1; }
    r->buffer->offset=0; uint32_t st=read_u32_from_pkg(r); eliminar_paquete(r); return (int)st;
}

#include "storage.h"

// utils cstring
static char* read_cstring(t_paquete* p){ int len=0; buffer_read(&len,p->buffer,sizeof(int)); char* s=calloc((size_t)len,1); if(len){ memcpy(s,p->buffer->stream+p->buffer->offset,(size_t)len); p->buffer->offset+=len; } return s; }
static uint32_t read_u32(t_paquete* p){ uint32_t v=0; buffer_read(&v,p->buffer,sizeof(uint32_t)); return v; }

static void respond_status(int fd, int opcode, uint32_t status){
    t_paquete* r = crear_paquete(opcode);
    agregar_a_paquete(r, &status, sizeof(uint32_t));
    enviar_paquete(r, fd); eliminar_paquete(r);
}

void handle_worker(int fd){
    for(;;){
        int op = recibir_operacion(fd);
        if(op <= 0){
            // desconectado
            pthread_mutex_lock(&m_workers);
            for(int i=0;i<list_size(g_workers);++i){ if((intptr_t)list_get(g_workers,i)==fd){ list_remove(g_workers,i); break; } }
            int cant=list_size(g_workers); pthread_mutex_unlock(&m_workers);
            log_worker_desconectado(0, cant); // WorkerID desconocido → 0
            close(fd);
            return;
        }
        t_paquete* pk = recibir_paquete(fd);
        if(!pk){ close(fd); return; }
        pk->buffer->offset = 0;

        delay_op();

        switch(op){
        case STORAGE_CREATE: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t st = op_create(0, file, tag);
            respond_status(fd, STORAGE_CREATE, st);
            free(file); free(tag);
        } break;

        case STORAGE_TRUNCATE: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t new_size = read_u32(pk);
            uint32_t st = op_truncate(0, file, tag, new_size);
            respond_status(fd, STORAGE_TRUNCATE, st);
            free(file); free(tag);
        } break;

        case STORAGE_DELETE: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t st = op_delete(0, file, tag);
            respond_status(fd, STORAGE_DELETE, st);
            free(file); free(tag);
        } break;

        case STORAGE_COMMIT: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t st = op_commit(0, file, tag);
            respond_status(fd, STORAGE_COMMIT, st);
            free(file); free(tag);
        } break;

        case STORAGE_TAG: {
            char* fsrc = read_cstring(pk); char* tsrc = read_cstring(pk);
            char* fdst = read_cstring(pk); char* tdst = read_cstring(pk);
            uint32_t st = op_tag(0, fsrc, tsrc, fdst, tdst);
            respond_status(fd, STORAGE_TAG, st);
            free(fsrc); free(tsrc); free(fdst); free(tdst);
        } break;

        case STORAGE_GET_BLOCK: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t logical = read_u32(pk);
            char* out=NULL;
            uint32_t st = op_get_block(0, file, tag, logical, &out);
            if(st==STATUS_OK){
                delay_block();
                // RESPUESTA: opcode STORAGE_GET_BLOCK + exactamente BLOCK_SIZE bytes
                t_paquete* r = crear_paquete(STORAGE_GET_BLOCK);
                agregar_a_paquete(r, out, g_block_size);
                enviar_paquete(r, fd); eliminar_paquete(r);
                free(out);
            } else {
                respond_status(fd, STORAGE_GET_BLOCK, st);
            }
            free(file); free(tag);
        } break;

        case STORAGE_PUT_BLOCK: {
            char* file = read_cstring(pk); char* tag = read_cstring(pk);
            uint32_t logical = read_u32(pk);
            uint32_t len = read_u32(pk);
            char* data = malloc(len);
            memcpy(data, pk->buffer->stream+pk->buffer->offset, len);
            pk->buffer->offset += len;

            delay_block();
            uint32_t st = op_put_block(0, file, tag, logical, data, len);
            respond_status(fd, STORAGE_PUT_BLOCK, st);
            free(file); free(tag); free(data);
        } break;

        default:
            // ignorar
            break;
        }

        eliminar_paquete(pk);
    }
}

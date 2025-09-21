// worker_master.c
#include "worker.h"


extern int g_fd_master;

static char* read_cstring_from_pkg(t_paquete* p){
    int len=0; buffer_read(&len, p->buffer, sizeof(int));
    char* s = calloc((size_t)len,1);
    if(len){ memcpy(s, p->buffer->stream + p->buffer->offset, (size_t)len); p->buffer->offset += len; }
    return s;
}

void* master_listener_thread(void* _){
    (void)_;
    for(;;){
        int op = recibir_operacion(g_fd_master);
        if(op <= 0){ log_error(g_wlogger,"Master se desconectó"); exit(1); }
        t_paquete* pkg = recibir_paquete(g_fd_master);
        if(!pkg){ log_error(g_wlogger,"Paquete nulo desde Master"); exit(1); }
        pkg->buffer->offset = 0;

        switch(op){
        case MASTER_ASIGNAR_QUERY: {
            uint32_t qid=0, pc=0; buffer_read(&qid, pkg->buffer, sizeof(uint32_t));
            buffer_read(&pc, pkg->buffer, sizeof(uint32_t));
            char* path = read_cstring_from_pkg(pkg);
            log_info(g_wlogger, "## Query %u: Se recibe la Query. El path de operaciones es: %s", qid, path);
            worker_exec_start(qid, pc, path);
            free(path);
        } break;

        case MASTER_DESALOJAR: {
            uint32_t qid=0; buffer_read(&qid, pkg->buffer, sizeof(uint32_t));
            log_info(g_wlogger, "## Query %u: Desalojada por pedido del Master", qid);
            worker_exec_request_preempt(qid);
        } break;

        default:
            log_warning(g_wlogger, "Opcode inesperado desde Master: %d", op);
        }

        eliminar_paquete(pkg);
    }
    return NULL;
}

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <commons/string.h>
#include <../../utils/src/utils/protocolos.h>
#include "master.h"

// QC envía [int32 network order][n bytes sin '\0']
static char* read_net_string_from_pkg(t_paquete* p){
    int32_t n_net=0; buffer_read(&n_net, p->buffer, sizeof(int32_t));
    int32_t n = ntohl(n_net);
    char* s = calloc((size_t)n+1,1);
    if(n>0){ memcpy(s, p->buffer->stream + p->buffer->offset, (size_t)n); p->buffer->offset += n; }
    return s;
}

void master_handle_qc(int fd){
    // espera QC_ENVIAR_QUERY
    int op = recibir_operacion(fd);
    if(op != QC_ENVIAR_QUERY){ close(fd); return; }
    t_paquete* pkg = recibir_paquete(fd);
    pkg->buffer->offset = 0;

    char* path = read_net_string_from_pkg(pkg);
    uint32_t prio=0; buffer_read(&prio, pkg->buffer, sizeof(uint32_t));
    eliminar_paquete(pkg);

    // crear t_query
    t_query* q = calloc(1, sizeof(*q));
    q->id = __sync_fetch_and_add(&g_next_qid, 1);  // autoincremental desde 0. :contentReference[oaicite:8]{index=8}
    q->path = path;
    q->prioridad = prio;
    q->pc = 0;
    q->qc_fd = fd;
    q->worker_fd = -1;
    q->estado = Q_READY;

    // guardar en diccionario y encolar READY
    pthread_mutex_lock(&m_queries);
    dictionary_put(g_queries, string_itoa(q->id), q);
    pthread_mutex_unlock(&m_queries);

    log_qc_conectado(q->path, q->prioridad, q->id);

    master_enqueue_ready(q);

    // mantener conexión abierta: leer hasta desconexión
    for(;;){
        int nxt = recibir_operacion(fd);
        if(nxt <= 0){
            // Desconexión de QC ⇒ cancelar su query
            log_qc_desconectado(q->id, q->prioridad); // y finalizar según estado. :contentReference[oaicite:10]{index=10}
            // Si estaba READY ⇒ EXIT directo; si EXEC ⇒ pedir desalojo al Worker primero. :contentReference[oaicite:11]{index=11}
            if(q->estado == Q_READY){
                q->estado = Q_EXIT;
            } else if(q->estado == Q_EXEC && q->worker_fd >= 0){
                send_master_desalojar(q->worker_fd, q->id);
                // Cuando vuelva WORKER_DEVOLVER_PC, el sched podría marcar EXIT, etc.
                q->estado = Q_EXIT;
            }
            close(fd);
            return;
        }
        // (podrías manejar mensajes futuros desde QC aquí)
        (void)recibir_paquete(fd); // descartar payload
    }
}

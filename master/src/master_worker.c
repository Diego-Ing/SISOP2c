#include <stdlib.h>
#include <unistd.h>
#include <commons/string.h>
#include <../../utils/src/utils/protocolos.h>
#include "master.h"

static uint32_t read_u32_from_pkg(t_paquete* p){
    uint32_t v=0; buffer_read(&v, p->buffer, sizeof(uint32_t)); return v;
}
static char* read_cstring_from_pkg(t_paquete* p){
    int len=0; buffer_read(&len, p->buffer, sizeof(int));
    char* s = calloc((size_t)len,1);
    if(len) { memcpy(s, p->buffer->stream + p->buffer->offset, (size_t)len); p->buffer->offset += len; }
    return s;
}

void master_handle_worker(int fd){
    // Recibir WORKER_IDENTIFICACION
    int opid = recibir_operacion(fd);
    if(opid != WORKER_IDENTIFICACION){ close(fd); return; }
    t_paquete* pkg = recibir_paquete(fd);
    pkg->buffer->offset = 0;
    uint32_t wid = read_u32_from_pkg(pkg);
    eliminar_paquete(pkg);

    // Registrar worker
    t_worker* w = calloc(1,sizeof(*w));
    w->id = wid; w->fd = fd; w->ocupado=false; w->running_qid=0xFFFFFFFF; w->next_q=NULL;

    pthread_mutex_lock(&m_workers);
    list_add(g_workers, w);
    pthread_mutex_unlock(&m_workers);

    log_worker_conectado(wid); // “## Se conecta el Worker <WORKER_ID> - Cantidad total de Workers: <CANTIDAD>” :contentReference[oaicite:12]{index=12}
    send_master_ack(fd);

    // Cada worker: loop de mensajes (LECTURA/FIN/PC...)
    for(;;){
        int op = recibir_operacion(fd);
        if(op <= 0){
            // desconexión de worker
            // Si tenía una query ejecutando, finaliza con error y notificar al QC. :contentReference[oaicite:13]{index=13}
            uint32_t qid_err = w->running_qid==0xFFFFFFFF? 0xFFFFFFFF : w->running_qid;
            log_worker_desconectado(w->id, qid_err); // “Se finaliza la Query <QUERY_ID> ...” :contentReference[oaicite:14]{index=14}
            if(qid_err != 0xFFFFFFFF){
                // buscar la query y marcar EXIT + avisar al QC si sigue conectado
                pthread_mutex_lock(&m_queries);
                t_query* q = dictionary_get(g_queries, string_itoa(qid_err));
                pthread_mutex_unlock(&m_queries);
                if(q && q->qc_fd>=0){
                    send_master_fin(q->qc_fd, "ERROR_WORKER_DESCONECTADO");
                }
            }

            // remover worker
            pthread_mutex_lock(&m_workers);
            for(int i=0;i<list_size(g_workers);++i){
                if(((t_worker*)list_get(g_workers,i))->fd == fd){
                    list_remove(g_workers,i); break;
                }
            }
            pthread_mutex_unlock(&m_workers);
            if(w->next_q){
                 master_enqueue_ready(w->next_q);
                 w->next_q = NULL;
            }
            close(fd);
            return;
        }

        t_paquete* pk = recibir_paquete(fd);
        pk->buffer->offset = 0;

        switch(op){
        case WORKER_LECTURA: {
            uint32_t qid = read_u32_from_pkg(pk);
            char* ft    = read_cstring_from_pkg(pk);
            char* cont  = read_cstring_from_pkg(pk);

            // reenviar al QC
            pthread_mutex_lock(&m_queries);
            t_query* q = dictionary_get(g_queries, string_itoa(qid));
            pthread_mutex_unlock(&m_queries);
            if(q && q->qc_fd>=0){
                send_master_lectura(q->qc_fd, ft, cont);
                log_envio_lectura_a_qc(qid, w->id); // “Se envía un mensaje de lectura ...” :contentReference[oaicite:15]{index=15}
            }
            free(ft); free(cont);
        } break;

        case WORKER_FIN: {
            uint32_t qid = read_u32_from_pkg(pk);
            char* motivo = read_cstring_from_pkg(pk);

            // marcar EXIT, liberar worker, notificar QC
            pthread_mutex_lock(&m_queries);
            t_query* q = dictionary_get(g_queries, string_itoa(qid));
            pthread_mutex_unlock(&m_queries);
            if(q){
                q->estado = Q_EXIT;
                if(q->qc_fd>=0) send_master_fin(q->qc_fd, motivo); // el QC loguea “## Query Finalizada - <MOTIVO>” :contentReference[oaicite:16]{index=16}
            }

            w->ocupado=false; w->running_qid=0xFFFFFFFF;
            log_fin_query_en_worker(qid, w->id); // “Se terminó la Query <QID> en el Worker <WID>” :contentReference[oaicite:17]{index=17}
        } break;

        case WORKER_DEVOLVER_PC: {
            uint32_t qid = read_u32_from_pkg(pk);
            uint32_t pc  = read_u32_from_pkg(pk);
            // almacenar PC para reanudación
            pthread_mutex_lock(&m_queries);
            t_query* q = dictionary_get(g_queries, string_itoa(qid));
            pthread_mutex_unlock(&m_queries);
            if(q){ q->pc = pc; q->estado = Q_READY; q->worker_fd=-1; }

            // la desalojada vuelve a READY
            if(q) master_enqueue_ready(q);

            // si había una Query pendiente para este worker (preempción), ¡asignarla ya!
            if(w->next_q){
                t_query* nq = w->next_q;
                w->next_q = NULL;
                nq->estado = Q_EXEC;
                nq->worker_fd = w->fd;
                w->ocupado = true; w->running_qid = nq->id;
                send_master_asignar_query(w->fd, nq->id, nq->pc, nq->path);
                log_envio_q_a_worker(nq->id, w->id);
            } else {
                // quedó libre
                w->ocupado=false; w->running_qid=0xFFFFFFFF;
            }


        } break;

        default:
            break;
        }
        eliminar_paquete(pk);
    }
}

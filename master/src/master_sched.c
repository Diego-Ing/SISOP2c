#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <commons/string.h>
#include <../../utils/src/utils/protocolos.h>
#include "master.h"
#include <time.h>



int master_count_workers(void){
    pthread_mutex_lock(&m_workers);
    int c = list_size(g_workers);
    pthread_mutex_unlock(&m_workers);
    return c;
}


uint64_t now_ms(void){
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);   // requiere C11
    return (uint64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

void master_enqueue_ready(t_query* q){
    q->last_aging_ms = now_ms();   // ← importante para aging individual
    pthread_mutex_lock(&m_ready);
    queue_push(g_ready, q);
    pthread_cond_signal(&c_ready);
    pthread_mutex_unlock(&m_ready);
}

t_query* master_pop_next_ready_fifo(void){
    pthread_mutex_lock(&m_ready);
    t_query* q = queue_pop(g_ready);
    pthread_mutex_unlock(&m_ready);
    return q;
}

void send_master_ack(int fd){
    t_paquete* p = crear_paquete(MASTER_ACK);
    enviar_paquete(p, fd);
    eliminar_paquete(p);
}

static void add_cstring(t_paquete* p, const char* s){
    int len = s ? (int)strlen(s) + 1 : 1;
    agregar_a_paquete(p, &len, sizeof(int));
    agregar_a_paquete(p, (void*)s, len);
}

void send_master_lectura(int qc_fd, const char* file_tag, const char* contenido){
    t_paquete* p = crear_paquete(MASTER_LECTURA);
    add_cstring(p, file_tag);
    add_cstring(p, contenido);
    enviar_paquete(p, qc_fd);
    eliminar_paquete(p);
}

void send_master_fin(int qc_fd, const char* motivo){
    t_paquete* p = crear_paquete(MASTER_FIN);
    add_cstring(p, motivo);
    enviar_paquete(p, qc_fd);
    eliminar_paquete(p);
}

void send_master_asignar_query(int worker_fd, uint32_t qid, uint32_t pc_inicial, const char* path){
    t_paquete* p = crear_paquete(MASTER_ASIGNAR_QUERY);
    agregar_a_paquete(p, &qid, sizeof(uint32_t));
    agregar_a_paquete(p, &pc_inicial, sizeof(uint32_t));
    add_cstring(p, path);
    enviar_paquete(p, worker_fd);
    eliminar_paquete(p);
}

void send_master_desalojar(int worker_fd, uint32_t qid){
    t_paquete* p = crear_paquete(MASTER_DESALOJAR);
    agregar_a_paquete(p, &qid, sizeof(uint32_t));
    enviar_paquete(p, worker_fd);
    eliminar_paquete(p);
}

// ===== Logs con el texto EXACTO pedido por la cátedra =====
// Master: conexiones y eventos de planificación/lecturas. :contentReference[oaicite:18]{index=18} :contentReference[oaicite:19]{index=19} :contentReference[oaicite:20]{index=20}
void log_qc_conectado(const char* path, uint32_t prio, uint32_t qid){
    int mp = master_count_workers(); // Nivel multiprocesamiento = cant de Workers conectados.
    log_info(g_logger, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %u - Id asignado: %u. Nivel multiprocesamiento %d",
              path, prio, qid, mp);
}
void log_qc_desconectado(uint32_t qid, uint32_t prio){
    int mp = master_count_workers();
    log_info(g_logger, "## Se desconecta un Query Control. Se finaliza la Query %u con prioridad %u. Nivel multiprocesamiento %d",
              qid, prio, mp);
}
void log_worker_conectado(uint32_t worker_id){
    int c = master_count_workers();
    log_info(g_logger, "## Se conecta el Worker %u - Cantidad total de Workers: %d", worker_id, c);
}
void log_worker_desconectado(uint32_t worker_id, uint32_t qid){
    int c = master_count_workers();
    if(qid==0xFFFFFFFF)
        log_info(g_logger, "## Se desconecta el Worker %u - Cantidad total de Workers: %d", worker_id, c);
    else
        log_info(g_logger, "## Se desconecta el Worker %u - Se finaliza la Query %u - Cantidad total de Workers: %d", worker_id, qid, c);
}
void log_envio_q_a_worker(uint32_t qid, uint32_t worker_id){
    log_info(g_logger, "## Se envía la Query %u al Worker %u", qid, worker_id);
}
void log_envio_lectura_a_qc(uint32_t qid, uint32_t worker_id){
    log_info(g_logger, "## Se envía un mensaje de lectura de la Query %u en el Worker %u al Query Control", qid, worker_id);
}
void log_fin_query_en_worker(uint32_t qid, uint32_t worker_id){
    log_info(g_logger, "## Se terminó la Query %u en el Worker %u", qid, worker_id);
}
void log_desalojo_por_prioridad(uint32_t qid_out, uint32_t prio_out, uint32_t qid_in, uint32_t prio_in, uint32_t worker_id){
    log_info(g_logger, "## Se desaloja la Query %u (%u) y comienza a ejecutar la Query %u (%u) en el Worker %u",
             qid_out, prio_out, qid_in, prio_in, worker_id);
}
void log_desalojo_por_desconexion(uint32_t qid, uint32_t worker_id){
    log_info(g_logger, "## Se desaloja la Query %u del Worker %u", qid, worker_id);
}
void log_cambio_prioridad(uint32_t qid, uint32_t p_old, uint32_t p_new){
    log_info(g_logger, "##%u Cambio de prioridad: %u - %u", qid, p_old, p_new);
}

#ifndef MASTER_H
#define MASTER_H

#include <stdint.h>
#include <stdbool.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <commons/collections/queue.h>
#include <arpa/inet.h>
#include <commons/collections/dictionary.h>
#include <../../utils/src/utils/conexiones.h>
#include <../../utils/src/utils/protocolos.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

// ===== Estados de una Query =====
typedef enum { Q_NEW, Q_READY, Q_EXEC, Q_EXIT } qstate_t;

// ===== Estructuras =====
typedef struct {
    uint32_t id;
    char*    path;        // path del archivo de Query
    uint32_t prioridad;   // 0 = mayor prioridad
    uint32_t pc;          // para reanudar tras desalojo
    int      qc_fd;       // socket del Query Control
    int      worker_fd;   // socket del Worker (si está en EXEC)
    uint64_t last_aging_ms;   // último instante en que se ageó / (re)encoló en READY
    qstate_t estado;
} t_query;

 typedef struct {
    uint32_t id;
    int      fd;
    bool     ocupado;
    uint32_t running_qid; // 0xFFFFFFFF si libre
    t_query* next_q; // si se desalojó otro para correr esta, se asigna apenas llega DEVOLVER_PC
 } t_worker;


// ===== Config =====
typedef struct {
    char* puerto_escucha;
    char* algoritmo;      // "FIFO" | "PRIORIDADES"
    int   tiempo_aging_ms; // 0 = sin aging
    t_log_level log_level;
} t_master_cfg;

// ======= Globals =======
extern t_log*        g_logger;
extern t_master_cfg  g_cfg;

extern int           g_server_fd;
extern uint32_t      g_next_qid;

extern t_queue*      g_ready;        // cola READY
extern t_list*       g_workers;      // lista de t_worker*
extern t_dictionary* g_queries;      // qid -> t_query*

extern pthread_mutex_t m_ready;
extern pthread_mutex_t m_workers;
extern pthread_mutex_t m_queries;
extern pthread_cond_t  c_ready;      // para despertar planificador

// ======= API =======
bool master_load_config(char* path);
void master_free_config(void);

void* master_accept_loop(void* arg);
void* master_scheduler_loop(void* arg);
void* master_aging_loop(void* arg); // opcional

// Handlers
void master_handle_qc(int fd);
void master_handle_worker(int fd);

// Utilidades
void master_enqueue_ready(t_query* q);
void master_assign_next_if_possible(void);
int  master_count_workers(void);
t_worker* master_pick_idle_worker(void);
t_query* master_pop_next_ready_fifo(void);
uint64_t now_ms(void);

// Protocolo
void send_master_ack(int fd);
void send_master_lectura(int qc_fd, const char* file_tag, const char* contenido);
void send_master_fin(int qc_fd, const char* motivo);
void send_master_asignar_query(int worker_fd, uint32_t qid, uint32_t pc_inicial, const char* path);
void send_master_desalojar(int worker_fd, uint32_t qid);

// Logs exactos pedido por consigna
void log_qc_conectado(const char* path, uint32_t prio, uint32_t qid);
void log_qc_desconectado(uint32_t qid, uint32_t prio);
void log_worker_conectado(uint32_t worker_id);
void log_worker_desconectado(uint32_t worker_id, uint32_t qid_en_ejec);
void log_envio_q_a_worker(uint32_t qid, uint32_t worker_id);
void log_envio_lectura_a_qc(uint32_t qid, uint32_t worker_id);
void log_fin_query_en_worker(uint32_t qid, uint32_t worker_id);
void log_desalojo_por_prioridad(uint32_t qid_out, uint32_t prio_out, uint32_t qid_in, uint32_t prio_in, uint32_t worker_id);
void log_desalojo_por_desconexion(uint32_t qid, uint32_t worker_id);
void log_cambio_prioridad(uint32_t qid, uint32_t prio_old, uint32_t prio_new);

#endif

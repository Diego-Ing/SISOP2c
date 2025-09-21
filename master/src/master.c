
#include "master.h"
#include "unistd.h"

// ====== Globals ======
t_log*        g_logger = NULL;
t_master_cfg  g_cfg;
int           g_server_fd = -1;
uint32_t      g_next_qid = 0;

t_queue*      g_ready = NULL;
t_list*       g_workers = NULL;
t_dictionary* g_queries = NULL;

pthread_mutex_t m_ready   = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m_workers = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t m_queries = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  c_ready   = PTHREAD_COND_INITIALIZER;

// ====== Config ======
bool master_load_config(char* path){
    t_config* cfg = config_create(path);
    if(!cfg) return false;
    g_cfg.puerto_escucha = strdup(config_get_string_value(cfg,"PUERTO_ESCUCHA"));
    g_cfg.algoritmo      = strdup(config_get_string_value(cfg,"ALGORITMO_PLANIFICACION")); // FIFO | PRIORIDADES
    g_cfg.tiempo_aging_ms= config_get_int_value(cfg,"TIEMPO_AGING");
    t_log_level lvl      = config_get_int_value(cfg,"LOG_LEVEL");
    g_logger = log_create("master.log","MASTER",1, lvl);
    config_destroy(cfg);
    return g_logger != NULL;
}
void master_free_config(void){
    free(g_cfg.puerto_escucha);
    free(g_cfg.algoritmo);
    if(g_logger) log_destroy(g_logger);
}

// ====== Señales ======
static void sigint_handler(int _sig){
    (void)_sig;
    if(g_logger) log_info(g_logger,"SIGINT: cerrando Master...");
    if(g_server_fd!=-1) close(g_server_fd);
    master_free_config();
    exit(0);
}

// ====== Accept loop ======
void* master_accept_loop(void* _arg){
    (void)_arg;
    for(;;){
        int fd = esperar_cliente(g_server_fd);
        if(fd < 0){ log_error(g_logger,"accept falló"); continue; }

        int op = recibir_operacion(fd);
        if(op <= 0){ close(fd); continue; }

        pthread_t th;
        if(op == HANDSHAKE_QC){
            send_master_ack(fd);
            pthread_create(&th,NULL,(void*(*)(void*))master_handle_qc,(void*)(intptr_t)fd);
        } else if(op == HANDSHAKE_WORKER){
            pthread_create(&th,NULL,(void*(*)(void*))master_handle_worker,(void*)(intptr_t)fd);
        } else {
            log_error(g_logger,"Handshake desconocido (%d)", op);
            close(fd);
            continue;
        }
        pthread_detach(th);
    }
    return NULL;
}

// master_sched.c
static t_query* pop_ready_siguiente(void){
    t_query* elegida = NULL;

    pthread_mutex_lock(&m_ready);
    int n = queue_size(g_ready);
    if(n == 0){ pthread_mutex_unlock(&m_ready); return NULL; }

    if(strcmp(g_cfg.algoritmo, "PRIORIDADES") != 0){
        // FIFO
        elegida = queue_pop(g_ready);
        pthread_mutex_unlock(&m_ready);
        return elegida;
    }

    // PRIORIDADES: buscamos la de menor prioridad numérica
    t_list* tmp = list_create();
    for(int i=0; i<n; ++i) list_add(tmp, queue_pop(g_ready));

    int idx_best = 0;
    uint32_t best = ((t_query*)list_get(tmp,0))->prioridad;
    for(int i=1; i<list_size(tmp); ++i){
        t_query* q = list_get(tmp,i);
        if(q->prioridad < best){ best = q->prioridad; idx_best = i; }
    }
    elegida = list_remove(tmp, idx_best);      // saco la mejor
    // devuelvo el resto a READY en el mismo orden
    for(int i=0; i<list_size(tmp); ++i) queue_push(g_ready, list_get(tmp,i));
    list_destroy(tmp);
    pthread_mutex_unlock(&m_ready);
    return elegida;
}

// ====== Scheduler loop (FIFO inicial) ======
 void* master_scheduler_loop(void* _arg){
     (void)_arg;
     for(;;){
         pthread_mutex_lock(&m_ready);
         while(queue_is_empty(g_ready)){
             pthread_cond_wait(&c_ready, &m_ready);
         }
         t_query* q = pop_ready_siguiente();
         pthread_mutex_unlock(&m_ready);

         // buscar un Worker libre
         pthread_mutex_lock(&m_workers);
         t_worker* w = NULL;
         for(int i=0;i<list_size(g_workers);++i){
             t_worker* cand = list_get(g_workers,i);
             if(!cand->ocupado){ w=cand; break; }
         }
         pthread_mutex_unlock(&m_workers);

        if(!w){
            // Sin worker libre: si algoritmo = PRIORIDADES, intentamos DESALOJAR
            if(strcmp(g_cfg.algoritmo, "PRIORIDADES") == 0){
                uint32_t prio_in = q->prioridad;
                t_worker* vict = NULL;
                t_query*  vict_q = NULL;
                uint32_t  worst = 0; // rastreamos la peor prioridad (mayor número)

                pthread_mutex_lock(&m_workers);
                for(int i=0;i<list_size(g_workers);++i){
                    t_worker* cw = list_get(g_workers,i);
                    if(!cw->ocupado) continue;
                    // buscar query en ejecución de este worker
                    pthread_mutex_lock(&m_queries);
                    char* key = string_itoa(cw->running_qid);
                    t_query* running = dictionary_get(g_queries, key);
                    free(key);
                    pthread_mutex_unlock(&m_queries);
                    if(!running) continue;
                    if(running->prioridad > prio_in && running->prioridad >= worst){
                        worst = running->prioridad;
                        vict = cw;
                        vict_q = running;
                    }
                }
                pthread_mutex_unlock(&m_workers);

                if(vict && vict_q){
                    // Desalojamos la peor y dejamos q pendiente en ese worker
                    send_master_desalojar(vict->fd, vict_q->id);
                    log_desalojo_por_prioridad(vict_q->id, vict_q->prioridad, q->id, q->prioridad, vict->id);
                    // marcaremos la q desalojada como READY cuando llegue WORKER_DEVOLVER_PC
                    pthread_mutex_lock(&m_workers);
                    vict->next_q = q; // asignar esta apenas devuelva PC
                    pthread_mutex_unlock(&m_workers);
                    // no reencolamos q: quedará “pendiente” en el worker víctima
                    continue;
                }
            }
            // Si no hubo desalojo posible, reencolamos
            master_enqueue_ready(q);
            usleep(50*1000);
            continue;
        }

         // asignar
         q->estado = Q_EXEC;
         q->worker_fd = w->fd;
         w->ocupado = true; w->running_qid = q->id;

         send_master_asignar_query(w->fd, q->id, q->pc, q->path);
         log_envio_q_a_worker(q->id, w->id);
     }
     return NULL;
 }
// ====== Aging loop (opcional) ======
void* master_aging_loop(void* _arg){
    (void)_arg;
    if(g_cfg.tiempo_aging_ms <= 0) return NULL;

    for(;;){
        usleep(100 * 1000); // 100ms de granularidad

        pthread_mutex_lock(&m_ready);
        int n = queue_size(g_ready);
        bool hubo_cambios = false;

        // volcamos la cola a una lista temporal, actualizamos y reconstruimos
        t_list* tmp = list_create();
        for(int i=0; i<n; ++i){
            t_query* q = queue_pop(g_ready);
            uint64_t elapsed = now_ms() - q->last_aging_ms;

            if(elapsed >= (uint64_t)g_cfg.tiempo_aging_ms){
                uint32_t prev = q->prioridad;
                // cuántos “saltos” de aging le corresponden por el tiempo acumulado
                uint64_t steps = elapsed / (uint64_t)g_cfg.tiempo_aging_ms;
                if(q->prioridad > steps) q->prioridad -= (uint32_t)steps;
                else q->prioridad = 0;

                q->last_aging_ms += steps * (uint64_t)g_cfg.tiempo_aging_ms;
                if(q->prioridad != prev){
                    log_cambio_prioridad(q->id, prev, q->prioridad);
                    hubo_cambios = true;
                }
            }
            list_add(tmp, q);
        }
        // reconstruimos la cola READY (sin necesidad de ordenar aquí)
        for(int i=0; i<list_size(tmp); ++i) queue_push(g_ready, list_get(tmp, i));
        list_destroy(tmp);
        pthread_mutex_unlock(&m_ready);

        if(hubo_cambios) pthread_cond_signal(&c_ready); // despertá al planificador
    }
    return NULL;
}

// ====== main ======
int main(int argc, char** argv){
    if(argc<2){ fprintf(stderr,"Uso: %s master.config\n", argv[0]); return EXIT_FAILURE; }
    if(!master_load_config(argv[1])){ fprintf(stderr,"No pude cargar config\n"); return EXIT_FAILURE; }

    signal(SIGINT, sigint_handler);

    g_ready   = queue_create();
    g_workers = list_create();
    g_queries = dictionary_create();

    g_server_fd = iniciar_servidor(g_cfg.puerto_escucha);
    if(g_server_fd < 0){ log_error(g_logger,"No pude iniciar servidor en %s", g_cfg.puerto_escucha); return EXIT_FAILURE; }
    log_info(g_logger,"Master escuchando en puerto %s", g_cfg.puerto_escucha);

    pthread_t th_accept, th_sched, th_aging;
    pthread_create(&th_accept, NULL, master_accept_loop, NULL);
    pthread_create(&th_sched,  NULL, master_scheduler_loop, NULL);
    pthread_detach(th_accept);
    pthread_detach(th_sched);

    if(g_cfg.tiempo_aging_ms > 0){
        pthread_create(&th_aging, NULL, master_aging_loop, NULL); // Aging READY periódicamente. :contentReference[oaicite:7]{index=7}
        pthread_detach(th_aging);
    }

    // dormir el main para siempre
    for(;;) pause();
    return 0;
}

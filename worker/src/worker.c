// worker.c

#include "worker.h"
// Definición de variables globales (sin extern)
t_log*   g_wlogger        = NULL;
int      g_fd_master      = -1;
int      g_fd_storage     = -1;
uint32_t g_block_size     = 0;
uint32_t g_worker_id      = 0;

size_t   g_mem_size_bytes = 0;
uint32_t g_mem_delay_ms   = 0;
t_reemplazo_algo g_reemplazo = REEMPLAZO_LRU;
char*    g_path_scripts   = NULL;


static void sigint_handler(int _sig){
    (void)_sig;
    if(g_wlogger) log_info(g_wlogger, "SIGINT recibido. Cerrando Worker...");
    if(g_fd_master!=-1)  close(g_fd_master);
    if(g_fd_storage!=-1) close(g_fd_storage);
    mem_destroy();
    if(g_wlogger) log_destroy(g_wlogger);
    _exit(0);
}

int main(int argc, char** argv){
    // ./bin/worker [archivo_config] [ID Worker]
    if(argc < 3){
        fprintf(stderr,"Uso: %s worker.config <WORKER_ID>\n", argv[0]);
        return EXIT_FAILURE;
    }
    g_worker_id = (uint32_t)strtoul(argv[2], NULL, 10);

    t_config* cfg = config_create(argv[1]);
    if(!cfg){ fprintf(stderr,"No pude leer %s\n", argv[1]); return EXIT_FAILURE; }

    const char* ip_master   = config_get_string_value(cfg, "IP_MASTER");
    const char* puerto_m    = config_get_string_value(cfg, "PUERTO_MASTER");
    const char* ip_storage  = config_get_string_value(cfg, "IP_STORAGE");
    const char* puerto_s    = config_get_string_value(cfg, "PUERTO_STORAGE");
    const char* lvl_s       = config_get_string_value(cfg, "LOG_LEVEL");
    t_log_level lvl         = log_level_from_string((char*)lvl_s);
    if (lvl == (t_log_level)-1) lvl = LOG_LEVEL_INFO;

    // Campos de memoria obligatorios
    g_mem_size_bytes = (size_t) config_get_int_value(cfg, "TAM_MEMORIA");
    g_mem_delay_ms   = (uint32_t)config_get_int_value(cfg, "RETARDO_MEMORIA");
    const char* repl = config_get_string_value(cfg, "ALGORITMO_REEMPLAZO");
    const char* path = NULL;
    // la consigna muestra PATH_SCRIPTS; por compatibilidad acepto PATH_QUERIES si existiera
    if(config_has_property(cfg,"PATH_SCRIPTS")) path = config_get_string_value(cfg,"PATH_SCRIPTS");
    else path = config_get_string_value(cfg,"PATH_QUERIES");
    g_path_scripts = strdup(path);

    g_wlogger = log_create("worker.log","WORKER",1, lvl);
    signal(SIGINT, sigint_handler);

    if(repl && strcmp(repl,"CLOCK-M")==0) g_reemplazo = REEMPLAZO_CLOCKM; else g_reemplazo = REEMPLAZO_LRU;

    log_info(g_wlogger, "Inicio Worker %u | MEM=%zuB | RETARDO=%ums | REEMPLAZO=%s | SCRIPTS=%s",
             g_worker_id, g_mem_size_bytes, g_mem_delay_ms,
             (g_reemplazo==REEMPLAZO_CLOCKM?"CLOCK-M":"LRU"), g_path_scripts);

    // 1) Storage: handshake → BLOCK_SIZE
    g_fd_storage = storage_connect_and_handshake(ip_storage, puerto_s);
    if(g_fd_storage < 0){ log_error(g_wlogger,"No pude conectar a Storage %s:%s", ip_storage, puerto_s); return EXIT_FAILURE; }

    // 2) Memoria Interna
    mem_init(g_mem_size_bytes, g_block_size, g_reemplazo, g_mem_delay_ms);

    // 3) Conectar a Master e identificarme
    g_fd_master = crear_conexion((char*)ip_master, (char*)puerto_m);
    if(g_fd_master < 0){ log_error(g_wlogger,"No pude conectar a Master %s:%s", ip_master, puerto_m); return EXIT_FAILURE; }
    t_paquete* hello = crear_paquete(HANDSHAKE_WORKER); enviar_paquete(hello, g_fd_master); eliminar_paquete(hello);
    t_paquete* who   = crear_paquete(WORKER_IDENTIFICACION);
    agregar_a_paquete(who, &g_worker_id, sizeof(uint32_t)); enviar_paquete(who, g_fd_master); eliminar_paquete(who);
    int op = recibir_operacion(g_fd_master); if(op==MASTER_ACK){ t_paquete* ack=recibir_paquete(g_fd_master); if(ack) eliminar_paquete(ack); }

    // 4) Ejecutar
    worker_exec_init();
    pthread_t t; pthread_create(&t, NULL, master_listener_thread, NULL); pthread_detach(t);

    for(;;) pause();
    return 0;
}

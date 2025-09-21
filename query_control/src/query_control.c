#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <signal.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <commons/log.h>
#include <commons/config.h>

#include <../../utils/src/utils/conexiones.h>
#include <../../utils/src/utils/protocolos.h>

// ====== Globals simples ======
static t_log* logger_qc = NULL;
static t_config* cfg_qc = NULL;
static int socket_master = -1;

// ====== Helpers de (de)serialización compatibles con tus paquetes ======
static void paquete_add_string(t_paquete* p, const char* s) {
    int32_t n = (int32_t)strlen(s);
    int32_t n_net = htonl(n);
    agregar_a_paquete(p, &n_net, sizeof(n_net));
    agregar_a_paquete(p, (void*)s, n);
}

static char* paquete_read_string(t_paquete* p){
    int32_t n_net = 0; buffer_read(&n_net, p->buffer, sizeof(n_net));
    int32_t n = ntohl(n_net);
    char* s = calloc((size_t)n+1, 1);
    memcpy(s, p->buffer->stream + p->buffer->offset, (size_t)n);
    p->buffer->offset += n;
    return s;
}


// ====== Señales: cerrar prolijo ======
static void sigint_handler(int _sig){
    (void)_sig;
    if(logger_qc) log_info(logger_qc, "SIGINT recibido. Cerrando Query Control...");
    if(socket_master != -1) close(socket_master);
    if(cfg_qc) config_destroy(cfg_qc);
    if(logger_qc) log_destroy(logger_qc);
    _exit(0);
}

// ====== Main ======
int main(int argc, char** argv)
{
    if(argc < 4){
        fprintf(stderr, "Uso: %s [archivo_config] [archivo_query] [prioridad]\n", argv[0]);
        return EXIT_FAILURE;
    }
    char* ruta_cfg   = argv[1];
    char* path_query = argv[2];
    uint32_t prioridad     = (uint32_t)strtoul(argv[3], NULL, 10);
    

    // Logger + config
    cfg_qc = config_create(ruta_cfg);
    if(!cfg_qc){ fprintf(stderr, "No pude leer config: %s\n", ruta_cfg); return EXIT_FAILURE; }

    const char* ip_master   = config_get_string_value(cfg_qc, "IP_MASTER");
    const char* puerto      = config_get_string_value(cfg_qc, "PUERTO_MASTER");
    const char* log_level_s = config_get_string_value(cfg_qc, "LOG_LEVEL");
    t_log_level lvl = log_level_from_string((char*)log_level_s);
    if (lvl == -1) lvl = LOG_LEVEL_INFO;
    logger_qc = log_create("query_control.log", "QUERY_CONTROL", true, lvl);


    // Señales
    signal(SIGINT, sigint_handler);

    socket_master = crear_conexion((char*)ip_master, (char*)puerto);
    if(socket_master < 0){
        log_error(logger_qc, "No pude conectar a Master %s:%s", ip_master, puerto);
        return EXIT_FAILURE;
    }
    log_info(logger_qc, "## Conexión al Master exitosa. IP: %s, Puerto: %s", ip_master, puerto);

    // Handshake QC
    {
        t_paquete* hello = crear_paquete(HANDSHAKE_QC);
        enviar_paquete(hello, socket_master);
        eliminar_paquete(hello);

        int op = recibir_operacion(socket_master);
        if (op <= 0) { log_error(logger_qc, "Master cerró durante handshake"); }
        t_paquete* ack = recibir_paquete(socket_master);
        eliminar_paquete(ack);
        if (op != MASTER_ACK) {
            log_error(logger_qc, "Handshake inválido. Esperaba MASTER_ACK (%d), llegó %d", MASTER_ACK, op);
        }
    }

    {
        t_paquete* p = crear_paquete(QC_ENVIAR_QUERY);
        paquete_add_string(p, path_query);   
        agregar_a_paquete(p, &prioridad, sizeof(uint32_t));
        enviar_paquete(p, socket_master);
        eliminar_paquete(p);

        log_info(logger_qc, "## Solicitud de ejecución de Query: %s, prioridad: %u",
                 path_query, prioridad);
    }
    for(;;){
        int op = recibir_operacion(socket_master);
        if(op <= 0){
            log_error(logger_qc, "Se cerró la conexión con Master o error de recv (op=%d).", op);
            break;
        }

        t_paquete* pkg = recibir_paquete(socket_master);
        if(!pkg){
            log_error(logger_qc, "Paquete nulo desde Master (op=%d).", op);
            break;
        }
        pkg->buffer->offset = 0;

        switch(op){
        case MASTER_LECTURA: {
            char* file_tag = paquete_read_string(pkg);
            char* contenido = paquete_read_string(pkg);
            log_info(logger_qc, "## Lectura realizada: Archivo %s, contenido: %s",
                     file_tag ? file_tag : "(null)", contenido ? contenido : "(null)");
            free(file_tag);
            free(contenido);
        } break;

        case MASTER_FIN: {
            char* motivo = paquete_read_string(pkg);
            log_info(logger_qc, "## Query Finalizada - %s", motivo ? motivo : "DESCONOCIDO");
            free(motivo);
            eliminar_paquete(pkg);
            close(socket_master);
            socket_master = -1;
            log_destroy(logger_qc);
            config_destroy(cfg_qc);
            return EXIT_SUCCESS;
        } break;

        case MASTER_ACK:
            // opcional: ACKs de control
            break;

        default:
            log_warning(logger_qc, "Opcode inesperado del Master: %d (descarto payload).", op);
            break;
        }

        eliminar_paquete(pkg);
    }

    // Limpieza en caso de error o cierre abrupto
    if(socket_master != -1) close(socket_master);
    if(cfg_qc) config_destroy(cfg_qc);
    if(logger_qc) log_destroy(logger_qc);
    return EXIT_FAILURE;
}

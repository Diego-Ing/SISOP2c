#ifndef PROTOCOLOS_H_
#define PROTOCOLOS_H_

#include <sys/types.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <string.h>
#include <readline/readline.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/list.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <semaphore.h>
#include <pthread.h>

typedef enum {
    // ---------------- Handshakes ----------------
    HANDSHAKE_QC             = 1000,
    HANDSHAKE_WORKER         = 1001,

    // ---------------- QC -> Master ----------------
    QC_ENVIAR_QUERY          = 1002,

    // ---------------- Worker -> Master (identificación) ----------------
    WORKER_IDENTIFICACION    = 1003,

    // ---------------- Master -> QC ----------------
    MASTER_ACK               = 1099,
    MASTER_LECTURA           = 1010,
    MASTER_FIN               = 1011,

    // ---------------- Master -> Worker ----------------
    MASTER_ASIGNAR_QUERY     = 2001,
    MASTER_DESALOJAR         = 2002,

    // ---------------- Worker -> Master ----------------
    WORKER_LECTURA           = 2101,
    WORKER_FIN               = 2102,
    WORKER_DEVOLVER_PC       = 2103,

    // ==============================================================
    //                    Worker <-> Storage
    // ==============================================================
    STORAGE_HANDSHAKE        = 3000,
    STORAGE_BLOCK_SIZE       = 3001,
    STORAGE_CREATE           = 3003,
    STORAGE_TRUNCATE         = 3004,
    STORAGE_DELETE           = 3006,
    STORAGE_COMMIT           = 3007,
    STORAGE_TAG              = 3008,
    STORAGE_GET_BLOCK        = 3010,
    STORAGE_PUT_BLOCK        = 3011
} op_code;


typedef enum {
    OK,
    NO,
    ERROR
} t_respuesta;

typedef struct
{
	int size;
	int offset;
	void *stream;
} t_buffer;

typedef struct
{
	op_code codigo_operacion;
	t_buffer *buffer;
} t_paquete;


int recibir_operacion(int);
t_paquete *crear_paquete(op_code );
void agregar_a_paquete(t_paquete *, void *, int );
int enviar_paquete(t_paquete *, int );
void eliminar_paquete(t_paquete *);
t_paquete* recibir_paquete(int );
void buffer_read(void* , t_buffer* , int ) ;
op_code obtener_codigo_instruccion(char*);
#endif

#include "protocolos.h"
#include <netdb.h>

t_paquete *crear_paquete(op_code codigo_operacion)
{
	t_paquete *paquete = malloc(sizeof(t_paquete));
	paquete->codigo_operacion = codigo_operacion;
	paquete->buffer = malloc(sizeof(t_buffer));
	paquete->buffer->size = 0;
	paquete->buffer->stream = NULL;
	return paquete;
}

void agregar_a_paquete(t_paquete *paquete, void *valor, int tamanio)
{
	paquete->buffer->stream = realloc(paquete->buffer->stream, paquete->buffer->size + tamanio);
	memcpy(paquete->buffer->stream + paquete->buffer->size, valor, tamanio);
	paquete->buffer->size += tamanio;
}

int enviar_paquete(t_paquete *paquete, int socket_destino)
{
	int bytes = paquete->buffer->size + sizeof(op_code) + sizeof(int);
	void *a_enviar = malloc(bytes);
	int offset = 0;

	memcpy(a_enviar + offset, &(paquete->codigo_operacion), sizeof(op_code));
	offset += sizeof(op_code);
	memcpy(a_enviar + offset, &(paquete->buffer->size), sizeof(int));
	offset += sizeof(int);
	memcpy(a_enviar + offset, paquete->buffer->stream, paquete->buffer->size);

	int result = send(socket_destino, a_enviar, bytes, 0);
	if (result < 0)
	{
		printf("Error al enviar el paquete\n");
	}
	free(a_enviar);
	return result;
}

void eliminar_paquete(t_paquete *paquete)
{
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

void buffer_read(void* dest, t_buffer* buffer, int size) {
    memcpy(dest, buffer->stream + buffer->offset, size);
    buffer->offset += size;
}

t_paquete* recibir_paquete(int socket) 
{
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->buffer = malloc(sizeof(t_buffer));
    // Recibir el tamaño del buffer
    recv(socket, &paquete->buffer->size, sizeof(int), 0);
    // Recibir el buffer
    paquete->buffer->stream = malloc(paquete->buffer->size);
    recv(socket, paquete->buffer->stream, paquete->buffer->size, 0);
    return paquete;
}
int recibir_operacion(int cliente_fd) {
    op_code codigo_operacion;
    int bytes_recibidos = recv(cliente_fd, &codigo_operacion, sizeof(op_code), 0);
    if (bytes_recibidos <= 0) {
        return -1;
    }
    return codigo_operacion;
}

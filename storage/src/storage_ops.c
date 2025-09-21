#include "storage.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>

static bool ensure_dirs_for_tag(const char* file, const char* tag){
    char* fd = path_file_dir(file); if(!ensure_dir(fd)){ free(fd); return false; }
    char* td = path_tag_dir(file,tag); if(!ensure_dir(td)){ free(fd); free(td); return false; }
    char* ld = path_tag_logical_dir(file,tag); bool ok=ensure_dir(ld);
    free(fd); free(td); free(ld);
    return ok;
}
static bool read_physical(uint32_t blk, char* out){
    char* p = path_block_n(blk);
    int fd = open(p, O_RDONLY);
    free(p);
    if (fd < 0) return false;

    size_t need = g_block_size;
    char* dst = out;
    while (need > 0) {
        ssize_t r = read(fd, dst, need);
        if (r < 0) { if (errno == EINTR) continue; close(fd); return false; }
        if (r == 0) break; // EOF inesperado
        dst  += (size_t)r;
        need -= (size_t)r;
    }
    close(fd);
    return need == 0;
}

static bool write_physical(uint32_t blk, const char* in, uint32_t len){
    char* p = path_block_n(blk);
    int fd = open(p, O_WRONLY);
    free(p);
    if (fd < 0) return false;

    if (ftruncate(fd, g_block_size) != 0) { close(fd); return false; }

    size_t left = len;
    const char* src = in;
    while (left > 0) {
        ssize_t w = write(fd, src, left);
        if (w < 0) { if (errno == EINTR) continue; close(fd); return false; }
        src  += (size_t)w;
        left -= (size_t)w;
    }

    if (len < g_block_size) {
        size_t pad = g_block_size - len;
        char* z = calloc(pad, 1);
        if (!z) { close(fd); return false; }
        size_t leftz = pad;
        char* pz = z;
        while (leftz > 0) {
            ssize_t w = write(fd, pz, leftz);
            if (w < 0) { if (errno == EINTR) continue; free(z); close(fd); return false; }
            pz    += (size_t)w;
            leftz -= (size_t)w;
        }
        free(z);
    }

    close(fd);
    return true;
}


static char* md5_block(const char* data, uint32_t len){
    // commons crypto_md5 devuelve char* heap con 32 hex (sin \n)
    return crypto_md5((char*)data, len);
}

uint32_t op_create(uint32_t qid, const char* file, const char* tag){
    delay_op();

    // evitar duplicados: ya existe metadata del mismo File:Tag
    char* mp = path_tag_metadata(file, tag);
    bool exists = (access(mp, F_OK) == 0);
    free(mp);
    if (exists) return ERR_NO_PERMITIDO;

    if(!ensure_dirs_for_tag(file,tag)) return ERR_IO;
    // metadata inicial
    t_tagmeta* m = calloc(1,sizeof(*m));
    m->size=0; m->estado=strdup("WORK_IN_PROGRESS"); m->blocks=list_create();
    if(!meta_save(file,tag,m)){ meta_destroy(m); return ERR_IO; }
    meta_destroy(m);
    log_file_creado(qid, file, tag);
    return STATUS_OK;
}

uint32_t op_truncate(uint32_t qid, const char* file, const char* tag, uint32_t new_size){
    delay_op();
    t_tagmeta* m = meta_load(file,tag); if(!m) return ERR_TAG_INEXISTENTE;
    if(strcmp(m->estado,"COMMITED")==0){ meta_destroy(m); return ERR_NO_PERMITIDO; }
    if(new_size % g_block_size){ meta_destroy(m); return ERR_FUERA_DE_LIMITE; }

    uint32_t cur_blocks = list_size(m->blocks);
    uint32_t new_blocks = new_size / g_block_size;

    // crecer
    if(new_blocks > cur_blocks){
        for(uint32_t i=cur_blocks; i<new_blocks; ++i){
            // cada nuevo lógico apunta a físico 0
            uint32_t* phys = malloc(sizeof(uint32_t)); *phys=0;
            list_add(m->blocks, phys);
            // logical link
            char* lp = path_tag_logical_block(file,tag,i);
            char* bp = path_block_n(0);
            ensure_hardlink(lp, bp);
            log_hl_agregado(qid, file, tag, i, 0);
            free(lp); free(bp);
        }
    }
    // achicar
    if(new_blocks < cur_blocks){
        for(int i=(int)cur_blocks-1; i>=(int)new_blocks; --i){
            uint32_t phys = *(uint32_t*)list_get(m->blocks, i);
            // eliminar hard link lógico
            char* lp = path_tag_logical_block(file,tag,(uint32_t)i);
            remove_logical_link(lp);
            log_hl_eliminado(qid, file, tag, (uint32_t)i, phys);
            free(lp);

            // si nadie más lo referencia, liberar bitmap
            if(physical_refcount(phys)==0 && phys!=0){
                bm_clear(phys);
                log_bf_liberado(qid, phys);
            }
            free(list_remove(m->blocks, i));
        }
    }

    m->size = new_size;
    meta_save(file,tag,m);
    log_file_truncado(qid, file, tag, new_size);
    meta_destroy(m);
    return STATUS_OK;
}

uint32_t op_tag(uint32_t qid, const char* fsrc, const char* tsrc, const char* fdst, const char* tdst){
    delay_op();
    t_tagmeta* ms = meta_load(fsrc,tsrc); if(!ms) return ERR_TAG_INEXISTENTE;
    if(!ensure_dirs_for_tag(fdst,tdst)){ meta_destroy(ms); return ERR_IO; }

    // copiar metadata
    t_tagmeta md = { .size = ms->size, .estado = "WORK_IN_PROGRESS", .blocks = list_create() };
    for(int i=0;i<list_size(ms->blocks);++i){
        uint32_t phys = *(uint32_t*)list_get(ms->blocks,i);
        uint32_t* np = malloc(sizeof(uint32_t)); *np = phys; list_add(md.blocks,np);
        // hard link lógico -> mismo bloque físico
        char* lp = path_tag_logical_block(fdst,tdst,(uint32_t)i);
        char* bp = path_block_n(phys);
        ensure_hardlink(lp,bp);
        log_hl_agregado(qid, fdst, tdst, (uint32_t)i, phys);
        free(lp); free(bp);
    }
    meta_save(fdst,tdst,&md);
    for(int i=0;i<list_size(md.blocks);++i) free(list_get(md.blocks,i));
    list_destroy(md.blocks);
    log_tag_creado(qid, fdst, tdst);
    meta_destroy(ms);
    return STATUS_OK;
}

uint32_t op_commit(uint32_t qid, const char* file, const char* tag){
    delay_op();
    t_tagmeta* m = meta_load(file,tag); if(!m) return ERR_TAG_INEXISTENTE;
    if(strcmp(m->estado,"COMMITED")==0){ meta_destroy(m); return STATUS_OK; }

    // por cada bloque lógico: leer data, calcular md5, buscar en índice; si existe otro físico => reasignar
    for(uint32_t i=0;i<(uint32_t)list_size(m->blocks);++i){
        uint32_t phys = *(uint32_t*)list_get(m->blocks,i);
        char* data = malloc(g_block_size);
        if(!read_physical(phys, data)){ free(data); meta_destroy(m); return ERR_IO; }
        char* md5 = md5_block(data, g_block_size);

        char* name = hi_get_block_by_md5(md5);
        if(name){
            // existe bloque físico confirmado con igual contenido → reasignar
            // nombre -> blockNNNN.dat ⇒ extraer número
            uint32_t target = (uint32_t)strtoul(name+5,NULL,10);
            if(target != phys){
                // reemplazar hard link lógico
                char* lp = path_tag_logical_block(file,tag,i);
                char* bp = path_block_n(target);
                replace_hardlink(lp,bp);
                log_dedupe(qid, file, tag, i, phys, target);
                free(lp); free(bp);
                // actualizar metadata
                *(uint32_t*)list_get(m->blocks,i) = target;
                // liberar anterior si quedó sin refs y no es 0
                if(physical_refcount(phys)==0 && phys!=0){
                    bm_clear(phys);
                    log_bf_liberado(qid, phys);
                }
            }
            free(name);
        } else {
            // no existe → registrar hash → este bloque queda como confirmado actual
            char* bname = string_from_format("block%04u.dat", phys);
            hi_put_md5(md5, bname); free(bname);
        }
        free(md5); free(data);
    }

    // evitar leak del estado previo
    free(m->estado);
    m->estado = strdup("COMMITED");

    meta_save(file, tag, m);
    hi_sync();
    log_commit(qid, file, tag);
    meta_destroy(m);
    return STATUS_OK;
}

uint32_t op_delete(uint32_t qid, const char* file, const char* tag){
    delay_op();
    if(strcmp(file,"initial_file")==0 && strcmp(tag,"BASE")==0) return ERR_NO_PERMITIDO;

    t_tagmeta* m = meta_load(file,tag); if(!m) return ERR_TAG_INEXISTENTE;

    // eliminar hard links y liberar físicos sin referencia
    for(int i=0;i<list_size(m->blocks);++i){
        uint32_t phys = *(uint32_t*)list_get(m->blocks,i);
        char* lp = path_tag_logical_block(file,tag,(uint32_t)i);
        remove_logical_link(lp);
        log_hl_eliminado(qid, file, tag, (uint32_t)i, phys);
        free(lp);
        if(physical_refcount(phys)==0 && phys!=0){
            bm_clear(phys);
            log_bf_liberado(qid, phys);
        }
    }
    // borrar directorio del tag
    char* td = path_tag_dir(file,tag);
    // metadata y logical dir
    char* mdp = path_tag_metadata(file,tag); unlink(mdp); free(mdp);
    char* ldp = path_tag_logical_dir(file,tag);
    DIR* d=opendir(ldp); if(d){ struct dirent* e; while((e=readdir(d))){ if(!strcmp(e->d_name,".")||!strcmp(e->d_name,"..")) continue; char* p = string_from_format("%s/%s", ldp, e->d_name); unlink(p); free(p);} closedir(d); }
    rmdir(ldp); free(ldp);
    rmdir(td); free(td);

    // si file queda vacío, removerlo
    char* fd = path_file_dir(file);
    DIR* df = opendir(fd);
    bool empty=true; if(df){ struct dirent* e; while((e=readdir(df))){ if(strcmp(e->d_name,".") && strcmp(e->d_name,"..")){ empty=false; break; } } closedir(df); }
    if(empty) rmdir(fd);
    free(fd);

    log_tag_eliminado(qid, file, tag);
    meta_destroy(m);
    return STATUS_OK;
}

uint32_t op_get_block(uint32_t qid, const char* file, const char* tag, uint32_t logical, char** out_data){
    t_tagmeta* m = meta_load(file,tag); if(!m) return ERR_TAG_INEXISTENTE;
    uint32_t blocks = (uint32_t)list_size(m->blocks);
    if(logical >= blocks){ meta_destroy(m); return ERR_FUERA_DE_LIMITE; }
    uint32_t phys = *(uint32_t*)list_get(m->blocks, logical);
    char* data = malloc(g_block_size);
    if(!read_physical(phys, data)){ free(data); meta_destroy(m); return ERR_IO; }
    *out_data = data;
    log_bloque_leido(qid, file, tag, logical);
    meta_destroy(m);
    return STATUS_OK;
}

uint32_t op_put_block(uint32_t qid, const char* file, const char* tag, uint32_t logical, const char* data, uint32_t len){
    t_tagmeta* m = meta_load(file,tag); if(!m) return ERR_TAG_INEXISTENTE;
    if(strcmp(m->estado,"COMMITED")==0){ meta_destroy(m); return ERR_NO_PERMITIDO; }
    uint32_t blocks = (uint32_t)list_size(m->blocks);
    if(logical >= blocks){ meta_destroy(m); return ERR_FUERA_DE_LIMITE; }

    uint32_t phys = *(uint32_t*)list_get(m->blocks, logical);
    uint32_t refs = physical_refcount(phys);

    // Si hay más de una referencia (o es bloque 0), asignar bloque nuevo
    if(refs > 1 || phys==0){
        int freeblk = bm_find_free(); if(freeblk<0){ meta_destroy(m); return ERR_SIN_ESPACIO; }
        bm_set((uint32_t)freeblk);
        log_bf_reservado(qid, (uint32_t)freeblk);

        // escribir data en el nuevo físico
        if(!write_physical((uint32_t)freeblk, data, len)){ meta_destroy(m); return ERR_IO; }

        // actualizar hard link lógico
        char* lp = path_tag_logical_block(file,tag,logical);
        char* nb = path_block_n((uint32_t)freeblk);
        replace_hardlink(lp, nb);
        log_hl_agregado(qid, file, tag, logical, (uint32_t)freeblk);
        free(lp); free(nb);

        // actualizar metadata
        *(uint32_t*)list_get(m->blocks, logical) = (uint32_t)freeblk;

        // liberar anterior si quedó sin refs y no es 0
        if(physical_refcount(phys)==0 && phys!=0){
            bm_clear(phys);
            log_bf_liberado(qid, phys);
        }
    } else {
        // única referencia: escribir directo sobre el mismo físico
        if(!write_physical(phys, data, len)){ meta_destroy(m); return ERR_IO; }
    }

    meta_save(file,tag,m);
    log_bloque_escrito(qid, file, tag, logical);
    meta_destroy(m);
    return STATUS_OK;
}

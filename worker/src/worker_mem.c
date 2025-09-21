// worker_mem.c

#include "worker.h"

typedef struct {
    char* file; char* tag; uint32_t page;
    bool  dirty;
    bool  ref;     // usado por CLOCK-M
    int   frame;   // índice de frame
} t_page;

static char* g_mem = NULL;
static uint32_t g_page_size = 0;
static int g_frames = 0;
static t_reemplazo_algo g_algo;
static uint32_t g_delay_ms;
static t_dictionary* g_ptable = NULL;   // key "file:tag#page" -> t_page*
static t_list* g_lru = NULL;            // lista de t_page* en orden LRU (cola = más reciente)
static int g_clk_hand = 0;              // índice circular para CLOCK-M (sobre vector frames)
static t_page** g_by_frame = NULL;      // frame->page*

static inline void mem_delay(void){
    usleep(g_delay_ms*1000); 
}

static inline int  frame_offset(int frame){ return frame * (int)g_page_size; }

static char* key_ftp(const char* f, const char* t, uint32_t p){
    char* ft = string_from_format("%s:%s#%u", f,t,p); return ft;
}
static void log_assign(uint32_t qid, int frame, const char* f, const char* t, uint32_t p){
    log_info(g_wlogger, "Query %u: Se asigna el Marco: %d a la Página: %u perteneciente al - File: %s - Tag: %s",
             qid, frame, p, f, t);
}
static void log_free_frame(uint32_t qid, int frame, const char* f, const char* t){
    log_info(g_wlogger, "Query %u: Se libera el Marco: %d perteneciente al - File: %s - Tag: %s",
             qid, frame, f, t);
}
static void log_miss(uint32_t qid, const char* f, const char* t, uint32_t p){
    log_info(g_wlogger, "Query %u: - Memoria Miss - File: %s - Tag: %s - Pagina: %u", qid, f, t, p);
}
static void log_add(uint32_t qid, const char* f, const char* t, uint32_t p, int frame){
    log_info(g_wlogger, "Query %u: - Memoria Add - File: %s - Tag: %s - Pagina: %u - Marco: %d", qid, f, t, p, frame);
}
static void log_reemplazo(uint32_t qid, const char* f1,const char* t1,uint32_t p1,
                          const char* f2,const char* t2,uint32_t p2){
    log_info(g_wlogger, "## Query %u: Se reemplaza la página %s:%s/%u por la %s:%s/%u",
             qid, f1,t1,p1, f2,t2,p2);
}
static void log_mem_rw(uint32_t qid, const char* accion, size_t phy, const char* val){
    log_info(g_wlogger, "Query %u: Acción: %s - Dirección Física: %zu - Valor: %s",
             qid, accion, phy, val?val:"");
}

void mem_init(size_t mem_bytes, uint32_t page_size, t_reemplazo_algo algo, uint32_t delay_ms){
    g_mem = calloc(mem_bytes,1);
    g_page_size = page_size;
    g_frames = (int)(mem_bytes / page_size);
    g_algo = algo;
    g_delay_ms = delay_ms;

    g_ptable = dictionary_create();
    g_lru    = list_create();
    g_by_frame = calloc(g_frames, sizeof(t_page*));
    g_clk_hand = 0;
}
void mem_destroy(void){
    if(!g_mem) return;
    for(int i=0;i<g_frames;i++) if(g_by_frame[i]){
        free(g_by_frame[i]->file); free(g_by_frame[i]->tag); free(g_by_frame[i]);
    }
    list_destroy(g_lru);
    dictionary_destroy(g_ptable);
    free(g_by_frame);
    free(g_mem);
    g_mem=NULL;
}

// buscar o cargar página; retorna t_page* y aplica logs/miss/add/asignación
static t_page* ensure_page(uint32_t qid, const char* f, const char* t, uint32_t p){
    char* k = key_ftp(f,t,p);
    t_page* pg = dictionary_get(g_ptable, k);
    if(pg){
        // LRU: mover a cola
        if(g_algo==REEMPLAZO_LRU){
            for(int i=0;i<list_size(g_lru);++i) if(list_get(g_lru,i)==pg){ list_remove(g_lru,i); break; }
            list_add(g_lru, pg);
        } else pg->ref = true; // CLOCK-M marca referencia
        free(k);
        return pg;
    }

    // Miss: cargar (si hay frame libre, usar; si no, elegir víctima)
    log_miss(qid, f,t,p);

    int frame_libre = -1;
    for(int i=0;i<g_frames;i++) if(!g_by_frame[i]) { frame_libre=i; break; }

    if(frame_libre == -1){
        // elegir víctima
        if(g_algo==REEMPLAZO_LRU){
            // víctima = cabeza de la lista
            t_page* vic = list_remove(g_lru, 0);
            frame_libre = vic->frame;

            if(vic->dirty){
                // flush de la víctima a Storage
                storage_put_block(vic->file, vic->tag, vic->page,
                                  g_mem + frame_offset(frame_libre), g_page_size);
            }
            log_reemplazo(qid, vic->file, vic->tag, vic->page, f,t,p);
            dictionary_remove_and_destroy(g_ptable, key_ftp(vic->file,vic->tag,vic->page), free);
            log_free_frame(qid, frame_libre, vic->file, vic->tag);
            free(vic->file); free(vic->tag); free(vic);
            g_by_frame[frame_libre]=NULL;
        } else {
            // CLOCK-M
            while(1){
                if(!g_by_frame[g_clk_hand]) { frame_libre = g_clk_hand; break; }
                t_page* cand = g_by_frame[g_clk_hand];
                if(cand->ref){ cand->ref=false; g_clk_hand = (g_clk_hand+1)%g_frames; continue; }
                // víctima
                t_page* vic = cand; frame_libre = vic->frame;
                if(vic->dirty){
                    storage_put_block(vic->file, vic->tag, vic->page,
                                      g_mem + frame_offset(frame_libre), g_page_size);
                }
                log_reemplazo(qid, vic->file, vic->tag, vic->page, f,t,p);
                // quitar de LRU si estuviera
                for(int i=0;i<list_size(g_lru);++i) if(list_get(g_lru,i)==vic){ list_remove(g_lru,i); break; }
                dictionary_remove_and_destroy(g_ptable, key_ftp(vic->file,vic->tag,vic->page), free);
                log_free_frame(qid, frame_libre, vic->file, vic->tag);
                free(vic->file); free(vic->tag); free(vic);
                g_by_frame[frame_libre]=NULL;
                g_clk_hand = (g_clk_hand+1)%g_frames;
                break;
            }
        }
    }

    // cargar desde Storage
    char* data = storage_get_block(f,t,p);
    if(!data){ // si Storage no tiene, trae cero
        data = calloc(g_page_size,1);
    }
    memcpy(g_mem + frame_offset(frame_libre), data, g_page_size);
    free(data);

    // crear page
    pg = calloc(1,sizeof(*pg));
    pg->file=strdup(f); pg->tag=strdup(t); pg->page=p; pg->dirty=false; pg->ref=true; pg->frame=frame_libre;
    g_by_frame[frame_libre] = pg;
    dictionary_put(g_ptable, key_ftp(f,t,p), pg);
    if(g_algo==REEMPLAZO_LRU) list_add(g_lru, pg);

    log_assign(qid, frame_libre, f,t,p);
    log_add(qid, f,t,p, frame_libre);
    return pg;
}

char* mem_read(uint32_t qid, const char* f, const char* t, size_t base, size_t size){
    size_t remaining = size, cursor = base;
    char* out = calloc(size+1,1);
    size_t out_off = 0;

    while(remaining > 0){
        uint32_t page = (uint32_t)(cursor / g_page_size);
        size_t   in_page_off = cursor % g_page_size;
        size_t   chunk = g_page_size - in_page_off;
        if(chunk > remaining) chunk = remaining;

        t_page* pg = ensure_page(qid, f,t,page);
        mem_delay();

        // leer
        size_t phy = (size_t)frame_offset(pg->frame) + in_page_off;
        memcpy(out + out_off, g_mem + phy, chunk);

        // log de acceso a memoria (muestra el fragmento leído)
        char* frag = strndup(out + out_off, (chunk>32)?32:chunk);
        log_mem_rw(qid, "LEER", phy, frag);
        free(frag);

        out_off += chunk; cursor += chunk; remaining -= chunk;
    }
    return out; // caller free
}

int mem_write(uint32_t qid, const char* f, const char* t, size_t base, const char* data, size_t len){
    size_t remaining = len, cursor = base;
    size_t src_off = 0;

    while(remaining > 0){
        uint32_t page = (uint32_t)(cursor / g_page_size);
        size_t   in_page_off = cursor % g_page_size;
        size_t   chunk = g_page_size - in_page_off;
        if(chunk > remaining) chunk = remaining;

        t_page* pg = ensure_page(qid, f,t,page);
        mem_delay();

        size_t phy = (size_t)frame_offset(pg->frame) + in_page_off;
        memcpy(g_mem + phy, data + src_off, chunk);
        pg->dirty = true;

        char* frag = strndup(data + src_off, (chunk>32)?32:chunk);
        log_mem_rw(qid, "ESCRIBIR", phy, frag);
        free(frag);

        src_off += chunk; cursor += chunk; remaining -= chunk;
    }
    return 0;
}

void mem_flush_file(uint32_t qid, const char* f, const char* t){
    // recorrer todos los frames y escribir los dirty que coincidan con file:tag
    for(int i=0;i<g_frames;i++){
        t_page* pg = g_by_frame[i];
        if(!pg) continue;
        if(strcmp(pg->file,f)==0 && strcmp(pg->tag,t)==0 && pg->dirty){
            storage_put_block(pg->file, pg->tag, pg->page, g_mem + frame_offset(i), g_page_size);
            pg->dirty=false;
        }
    }
    (void)qid; // los logs de flush explícito no eran obligatorios, ya logueamos escrituras
}

void mem_flush_set(uint32_t qid, t_list* touched){
    // touched: lista de char* "file:tag"
    for(int k=0;k<list_size(touched);++k){
        char* ft = list_get(touched,k);
        char* file = strdup(ft);
        char* sep = strchr(file, ':');
        if(!sep){ free(file); continue; }
        *sep='\0'; char* tag = sep+1;
        mem_flush_file(qid, file, tag);
        free(file);
    }
}

void mem_drop_file(const char* f, const char* t){
    // libera frames (sin flush, el que llama decide si flush o no) + LOG obligatorio
    for(int i=0;i<g_frames;i++){
        t_page* pg = g_by_frame[i];
        if(!pg) continue;
        if(strcmp(pg->file,f)==0 && strcmp(pg->tag,t)==0){
            // quitar de ptable y LRU
            for(int j=0;j<list_size(g_lru);++j) if(list_get(g_lru,j)==pg){ list_remove(g_lru,j); break; }
            dictionary_remove_and_destroy(g_ptable, key_ftp(pg->file,pg->tag,pg->page), free);
            log_free_frame(0 /*qid no relevante*/, i, pg->file, pg->tag);
            free(pg->file); free(pg->tag); free(pg);
            g_by_frame[i]=NULL;
        }
    }
}
// Invalidar todas las páginas de file:tag con número >= first_page (TRUNCATE que achica)
void mem_invalidate_from_page(uint32_t qid, const char* f, const char* t, uint32_t first_page){
    for(int i=0;i<g_frames;i++){
        t_page* pg = g_by_frame[i];
        if(!pg) continue;
        if(strcmp(pg->file,f)==0 && strcmp(pg->tag,t)==0 && pg->page >= first_page){
            // quitar de estructuras
            for(int j=0;j<list_size(g_lru);++j) if(list_get(g_lru,j)==pg){ list_remove(g_lru,j); break; }
            dictionary_remove_and_destroy(g_ptable, key_ftp(pg->file,pg->tag,pg->page), free);
            // No se flushea: el Storage ya truncó, estas páginas quedan fuera del nuevo tamaño
            log_free_frame(qid, i, pg->file, pg->tag);
            free(pg->file); free(pg->tag); free(pg);
            g_by_frame[i]=NULL;
        }
    }
}
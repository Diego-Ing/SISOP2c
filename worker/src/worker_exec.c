// worker_exec.c

#include "worker.h"

static inline void add_cstring(t_paquete* p, const char* s){ int len=s?(int)strlen(s)+1:1; agregar_a_paquete(p,&len,sizeof(int)); agregar_a_paquete(p,(void*)s,len); }
static void send_worker_lectura(uint32_t qid, const char* filetag, const char* contenido){
    t_paquete* pk = crear_paquete(WORKER_LECTURA); agregar_a_paquete(pk,&qid,sizeof(uint32_t));
    add_cstring(pk,filetag); add_cstring(pk,contenido); enviar_paquete(pk,g_fd_master); eliminar_paquete(pk);
}
static void send_worker_fin(uint32_t qid, const char* motivo){
    t_paquete* pk = crear_paquete(WORKER_FIN); agregar_a_paquete(pk,&qid,sizeof(uint32_t));
    add_cstring(pk,motivo); enviar_paquete(pk,g_fd_master); eliminar_paquete(pk);
}
static void send_worker_devolver_pc(uint32_t qid, uint32_t pc){
    t_paquete* pk = crear_paquete(WORKER_DEVOLVER_PC); agregar_a_paquete(pk,&qid,sizeof(uint32_t));
    agregar_a_paquete(pk,&pc,sizeof(uint32_t)); enviar_paquete(pk,g_fd_master); eliminar_paquete(pk);
}

// -------- Estado de ejecución --------
typedef struct {
    uint32_t qid;
    uint32_t pc;
    char*    path;
    pthread_t thread;
    bool     running;
    bool     preempt;
    pthread_mutex_t mx;
    t_list*  touched;    // lista de char* "file:tag" modificados (para flush por desalojo)
} t_exec;

static t_exec g_exec = {0};

// helpers
static char* join_path(const char* dir, const char* name){
    if(!name) return strdup("");
    if(name[0]=='/' || strchr(name,'/')) return strdup(name);
    size_t n = strlen(dir)+1+strlen(name)+1; char* r=malloc(n); snprintf(r,n,"%s/%s",dir,name); return r;
}
static void touched_add(const char* file, const char* tag){
    char* ft = string_from_format("%s:%s", file, tag);
    for(int i=0;i<list_size(g_exec.touched);++i){
        if(strcmp(list_get(g_exec.touched,i),ft)==0){ free(ft); return; }
    }
    list_add(g_exec.touched, ft);
}

// parsing file:tag y números
static bool split_filetag(const char* in, char** file, char** tag){
    char* c = strdup(in); char* sep = strchr(c, ':'); if(!sep){ free(c); return false; }
    *sep='\0'; *file=strdup(c); *tag=strdup(sep+1); free(c); return true;
}

// ---- Parser de líneas ----
typedef enum {I_CREATE, I_TRUNCATE, I_WRITE, I_READ, I_TAG, I_COMMIT, I_FLUSH, I_DELETE, I_END, I_UNKNOWN} instr_t;
static instr_t parse_line(char* line, char*** argv, int* argc){
    *argc=0; *argv=NULL;
    if(!line) return I_UNKNOWN;
    char* c = string_duplicate(line); string_trim(&c);
    if(string_is_empty(c) || string_starts_with(c,"#")){ free(c); return I_UNKNOWN; }

    // tokenizar por espacios
    char** toks = string_split(c, " ");
    free(c);
    for(; toks[*argc]; (*argc)++);

    if(strcmp(toks[0],"CREATE")==0)   { *argv=toks; return I_CREATE;   }
    if(strcmp(toks[0],"TRUNCATE")==0) { *argv=toks; return I_TRUNCATE; }
    if(strcmp(toks[0],"WRITE")==0)    { *argv=toks; return I_WRITE;    }
    if(strcmp(toks[0],"READ")==0)     { *argv=toks; return I_READ;     }
    if(strcmp(toks[0],"TAG")==0)      { *argv=toks; return I_TAG;      }
    if(strcmp(toks[0],"COMMIT")==0)   { *argv=toks; return I_COMMIT;   }
    if(strcmp(toks[0],"FLUSH")==0)    { *argv=toks; return I_FLUSH;    }
    if(strcmp(toks[0],"DELETE")==0)   { *argv=toks; return I_DELETE;   }
    if(strcmp(toks[0],"END")==0)      { *argv=toks; return I_END;      }

    // default
    for (int i = 0; i < *argc; i++) {
        free(toks[i]);
    }
    free(toks); 
    *argc = 0; 
    *argv = NULL;
    return I_UNKNOWN;

}
static void free_args(char** argv, int argc){ for(int i=0;i<argc;i++) free(argv[i]); free(argv); }

// ---- Hilo de ejecución ----
static void* run(void* _){
    (void)_;
    pthread_mutex_lock(&g_exec.mx);
    uint32_t qid=g_exec.qid, pc=g_exec.pc; char* path=join_path(g_path_scripts, g_exec.path);
    pthread_mutex_unlock(&g_exec.mx);

    // abrir script
    FILE* f = fopen(path,"r");
    if(!f){ log_error(g_wlogger,"No puedo abrir script %s", path); send_worker_fin(qid,"ERROR_OPEN_QUERY"); free(path); goto end; }

    // recorrer líneas
    char* line = NULL; size_t n=0; ssize_t r;
    // saltar hasta PC
    for(uint32_t i=0; i<pc && (r=getline(&line,&n,f))!=-1; i++);
    while((r=getline(&line,&n,f))!=-1){
        if(r>0 && line[r-1]=='\n') line[r-1]=0;

        // log FETCH
        log_info(g_wlogger, "## Query %u: FETCH - Program Counter: %u - %s", qid, pc, line);

        // check desalojo
        pthread_mutex_lock(&g_exec.mx); bool pre=g_exec.preempt; pthread_mutex_unlock(&g_exec.mx);
        if(pre){
            // flush implícito de los modificados
            mem_flush_set(qid, g_exec.touched);
            send_worker_devolver_pc(qid, pc);
            break;
        }

        // parseo y ejecución
        char** argv=NULL; int argc=0; instr_t in = parse_line(line,&argv,&argc);
        if(in==I_UNKNOWN){ pc++; free_args(argv,argc); continue; }

        switch(in){
        case I_CREATE: {
            if(argc<2){ send_worker_fin(qid,"ERROR_ARGS_CREATE"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            if(storage_create(file,tag)!=0){ send_worker_fin(qid,"ERROR_STORAGE_CREATE"); free(file); free(tag); free_args(argv,argc); goto fin; }
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: CREATE %s:%s", qid,file,tag);
            free(file); free(tag); pc++;
        } break;

        case I_TRUNCATE: {
            if(argc<3){ send_worker_fin(qid,"ERROR_ARGS_TRUNCATE"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            uint32_t sz=(uint32_t)strtoul(argv[2],NULL,10);
            if(sz % g_block_size){ send_worker_fin(qid,"ERROR_TRUNCATE_MULTIPLE"); free(file); free(tag); free_args(argv,argc); goto fin; }

            if(storage_truncate(file,tag,sz)!=0){ send_worker_fin(qid,"ERROR_STORAGE_TRUNCATE"); free(file); free(tag); free_args(argv,argc); goto fin; }
            // Si achica: invalidar páginas >= new_pages (no se flushean; quedan fuera del tamaño)
            {
                uint32_t new_pages = (sz / g_block_size);
                mem_invalidate_from_page(qid, file, tag, new_pages);
            }
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: TRUNCATE %s:%s %u", qid,file,tag,sz);
            free(file); free(tag); pc++;
        } break;

        case I_WRITE: {
            if(argc<4){ send_worker_fin(qid,"ERROR_ARGS_WRITE"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            size_t base=(size_t)strtoull(argv[2],NULL,10);
            const char* contenido = argv[3];
            mem_write(qid, file, tag, base, contenido, strlen(contenido));
            touched_add(file,tag);
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: WRITE %s:%s %zu \"%s\"", qid,file,tag,base,contenido);
            free(file); free(tag); pc++;
        } break;

        case I_READ: {
            if(argc<4){ send_worker_fin(qid,"ERROR_ARGS_READ"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            size_t base=(size_t)strtoull(argv[2],NULL,10);
            size_t size=(size_t)strtoull(argv[3],NULL,10);
            char* out = mem_read(qid, file, tag, base, size);
            char* ft = string_from_format("%s:%s", file, tag);
            send_worker_lectura(qid, ft, out);
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: READ %s:%s %zu %zu", qid,file,tag,base,size);
            free(ft); free(out); free(file); free(tag); pc++;
        } break;

        case I_TAG: {
            if(argc<3){ send_worker_fin(qid,"ERROR_ARGS_TAG"); free_args(argv,argc); goto fin; }
            // TAG f1:t1 f2:t2
            char *f1=NULL,*t1=NULL,*f2=NULL,*t2=NULL;
            if(!split_filetag(argv[1],&f1,&t1) || !split_filetag(argv[2],&f2,&t2)){
                send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin;
            }
            if(storage_tag(f1,t1,f2,t2)!=0){ send_worker_fin(qid,"ERROR_STORAGE_TAG"); free(f1); free(t1); free(f2); free(t2); free_args(argv,argc); goto fin; }
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: TAG %s:%s -> %s:%s", qid,f1,t1,f2,t2);
            free(f1); free(t1); free(f2); free(t2); pc++;
        } break;

        case I_COMMIT: {
            if(argc<2){ send_worker_fin(qid,"ERROR_ARGS_COMMIT"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            // FLUSH implícito
            mem_flush_file(qid,file,tag);
            if(storage_commit(file,tag)!=0){ send_worker_fin(qid,"ERROR_STORAGE_COMMIT"); free(file); free(tag); free_args(argv,argc); goto fin; }
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: COMMIT %s:%s", qid,file,tag);
            free(file); free(tag); pc++;
        } break;

        case I_FLUSH: {
            if(argc<2){ send_worker_fin(qid,"ERROR_ARGS_FLUSH"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            mem_flush_file(qid,file,tag);
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: FLUSH %s:%s", qid,file,tag);
            free(file); free(tag); pc++;
        } break;

        case I_DELETE: {
            if(argc<2){ send_worker_fin(qid,"ERROR_ARGS_DELETE"); free_args(argv,argc); goto fin; }
            char *file=NULL,*tag=NULL; if(!split_filetag(argv[1],&file,&tag)){ send_worker_fin(qid,"ERROR_FILETAG"); free_args(argv,argc); goto fin; }
            // recomendable: flush antes de borrar
            mem_flush_file(qid,file,tag);
            if(storage_delete(file,tag)!=0){ send_worker_fin(qid,"ERROR_STORAGE_DELETE"); free(file); free(tag); free_args(argv,argc); goto fin; }
            mem_drop_file(file,tag);
            log_info(g_wlogger, "## Query %u: - Instrucción realizada: DELETE %s:%s", qid,file,tag);
            free(file); free(tag); pc++;
        } break;

        case I_END:
            // FIN de la Query
            send_worker_fin(qid, "OK");
            goto fin;
        }
        free_args(argv,argc);
        // actualizar PC compartido
        pthread_mutex_lock(&g_exec.mx); g_exec.pc = pc; pthread_mutex_unlock(&g_exec.mx);
    }

fin:
    free(line); fclose(f); free(path);
end:
    pthread_mutex_lock(&g_exec.mx);
    g_exec.running=false;
    // liberar lista touched
    for(int i=0;i<list_size(g_exec.touched);++i) free(list_get(g_exec.touched,i));
    list_clean(g_exec.touched);
    pthread_mutex_unlock(&g_exec.mx);
    return NULL;
}

// API
void worker_exec_init(void){
    memset(&g_exec,0,sizeof(g_exec));
    pthread_mutex_init(&g_exec.mx,NULL);
    g_exec.touched = list_create();
}
void worker_exec_shutdown(void){
    for(int i=0;i<list_size(g_exec.touched);++i) free(list_get(g_exec.touched,i));
    list_destroy(g_exec.touched);
    pthread_mutex_destroy(&g_exec.mx);
}
void worker_exec_start(uint32_t qid, uint32_t pc_inicial, const char* path_query){
    pthread_mutex_lock(&g_exec.mx);
    if(g_exec.running){ log_error(g_wlogger,"Asignación recibida con Worker ocupado."); pthread_mutex_unlock(&g_exec.mx); return; }
    g_exec.qid=qid; g_exec.pc=pc_inicial;
    free(g_exec.path); g_exec.path=strdup(path_query);
    g_exec.preempt=false; g_exec.running=true;
    pthread_create(&g_exec.thread,NULL,run,NULL); pthread_detach(g_exec.thread);
    pthread_mutex_unlock(&g_exec.mx);
}
void worker_exec_request_preempt(uint32_t qid){
    pthread_mutex_lock(&g_exec.mx);
    if(g_exec.running && g_exec.qid==qid) g_exec.preempt=true;
    pthread_mutex_unlock(&g_exec.mx);
}

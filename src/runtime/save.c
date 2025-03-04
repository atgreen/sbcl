/*
 * This software is part of the SBCL system. See the README file for
 * more information.
 *
 * This software is derived from the CMU CL system, which was
 * written at Carnegie Mellon University and released into the
 * public domain. The software is in the public domain and is
 * provided with absolutely no warranty. See the COPYING and CREDITS
 * files for more information.
 */

#ifndef LISP_FEATURE_WIN32
#include <sys/types.h>
#include <sys/stat.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>

#include "sbcl.h"
#ifdef LISP_FEATURE_WIN32
#include "pthreads_win32.h"
#else
#include <signal.h>
#endif
#include "runtime.h"
#include "os.h"
#include "core.h"
#include "globals.h"
#include "save.h"
#include "dynbind.h"
#include "lispregs.h"
#include "validate.h"
#include "gc.h"
#include "thread.h"
#include "arch.h"
#include "genesis/static-symbols.h"
#include "genesis/symbol.h"
#include "genesis/vector.h"
#include "immobile-space.h"
#include "search.h"

#ifdef LISP_FEATURE_SB_CORE_COMPRESSION
# include <zstd.h>
#endif

#define GENERAL_WRITE_FAILURE_MSG "error writing to core file"

/* write_memsize_options uses a simple serialization scheme that
 * consists of one word of magic, one word indicating the size of the
 * core entry, and one word per struct field. */
static void
write_memsize_options(FILE *file)
{
    core_entry_elt_t optarray[RUNTIME_OPTIONS_WORDS] = {
      RUNTIME_OPTIONS_MAGIC,
      5, // number of words in this core header entry
      dynamic_space_size,
      thread_control_stack_size,
      dynamic_values_bytes
    };

    if (RUNTIME_OPTIONS_WORDS !=
        fwrite(optarray, sizeof(core_entry_elt_t), RUNTIME_OPTIONS_WORDS, file)) {
        perror("Error writing runtime options to file");
    }
}

static void
write_lispobj(lispobj obj, FILE *file)
{
    if (1 != fwrite(&obj, sizeof(lispobj), 1, file)) {
        perror(GENERAL_WRITE_FAILURE_MSG);
    }
}

static void
write_bytes_to_file(FILE * file, char *addr, size_t bytes, int compression)
{
    if (compression == COMPRESSION_LEVEL_NONE) {
        while (bytes > 0) {
            sword_t count = fwrite(addr, 1, bytes, file);
            if (count > 0) {
                bytes -= count;
                addr += count;
            }
            else {
                perror(GENERAL_WRITE_FAILURE_MSG);
                lose("core file is incomplete or corrupt");
            }
        }
#ifdef LISP_FEATURE_SB_CORE_COMPRESSION
    } else if ((compression >= -7) && (compression <= 22)) {
        int ret;
        ZSTD_inBuffer input;
        input.src = addr;
        input.size = bytes;
        input.pos = 0;

        size_t buf_size = ZSTD_CStreamOutSize();
        unsigned char* buf = successful_malloc(buf_size);
        ZSTD_outBuffer output;
        output.dst = buf;
        output.size = buf_size;

        unsigned char * written, * end;
        long total_written = 0;
        ZSTD_CStream *stream = ZSTD_createCStream();
        if (stream == NULL)
            lose("failed to create zstd compression context");
        ret = ZSTD_initCStream(stream, compression);
        if (ZSTD_isError(ret))
            lose("ZSTD_initCStream failed with error: %s",
                 ZSTD_getErrorName(ret));
        do {
            output.pos = 0;
            if (input.pos < bytes)
                ret = ZSTD_compressStream(stream, &output, &input);
            else
                ret = ZSTD_endStream(stream, &output);
            if (ZSTD_isError(ret))
                lose("ZSTD_compressStream2 failed with error: %s",
                     ZSTD_getErrorName(ret));
            written = buf;
            end = buf + output.pos;
            total_written += output.pos;
            while (written < end) {
                long count = fwrite(written, 1, output.pos, file);
                if (count > 0) {
                    written += count;
                } else {
                    perror(GENERAL_WRITE_FAILURE_MSG);
                    lose("core file is incomplete or corrupt");
                }
            }
        } while (ret != 0);
        printf("compressed %lu bytes into %lu at level %i\n",
               bytes, total_written, compression);

        ZSTD_freeCStream(stream);
#endif
    } else {
#ifdef LISP_FEATURE_SB_CORE_COMPRESSION
        lose("Unknown core compression level %i, exiting", compression);
#else
        lose("zstd-compressed core support not built in this runtime");
#endif
    }

    if (fflush(file) != 0) {
      perror(GENERAL_WRITE_FAILURE_MSG);
      lose("core file is incomplete or corrupt");
    }
};

static long write_bytes(FILE *file, char *addr, size_t bytes,
                        os_vm_offset_t file_offset, int compression)
{
    ftell_type here, data;

#ifdef LISP_FEATURE_WIN32
    // I can't see how we'd ever attempt writing from uncommitted memory,
    // but this is better than was was previously here (a "touch" loop over all pages)
    os_commit_memory(addr, bytes);
#endif

    fflush(file);
    here = FTELL(file);
    FSEEK(file, 0, SEEK_END);
    data = ALIGN_UP(FTELL(file), os_vm_page_size);
    FSEEK(file, data, SEEK_SET);
    write_bytes_to_file(file, addr, bytes, compression);
    FSEEK(file, here, SEEK_SET);
    return ((data - file_offset) / os_vm_page_size) - 1;
}

static void
output_space(FILE *file, int id, lispobj *addr, lispobj *end,
             os_vm_offset_t file_offset,
             int core_compression_level)
{
    size_t words, bytes, data, compressed_flag;
    static char *names[] = {NULL, "dynamic", "static", "read-only",
                            "fixedobj", "text"};

    compressed_flag
            = ((core_compression_level != COMPRESSION_LEVEL_NONE)
               ? DEFLATED_CORE_SPACE_ID_FLAG : 0);

    write_lispobj(id | compressed_flag, file);
    words = end - addr;
    write_lispobj(words, file);

    bytes = words * sizeof(lispobj);

#ifdef LISP_FEATURE_CHENEYGC
    /* KLUDGE: cheneygc can not restart a saved core if the dynamic space is empty,
     * because coreparse would never get to make the second semispace. That GC is such
     * a total piece of garbage that I don't care to fix, but yet it shouldn't be in
     * such bad shape that saved cores don't work. This seems to do the trick. */
    if (id == DYNAMIC_CORE_SPACE_ID && bytes == 0) bytes = 2*N_WORD_BYTES;
#endif

    if (!lisp_startup_options.noinform)
        printf("writing %lu bytes from the %s space at %p\n",
               (long unsigned)bytes, names[id], addr);

    /* FIXME: it sure would be nice to discover and document the behavior of this function
     * with regard to aligning up the byte count as pertains to bytes spanned by a rounded
     * up count that were not zeroized and would not have been written had we not rounded.
     * That seems quite bogus to operate on bytes that the caller didn't promise were OK
     * to be saved out (and didn't contain, say, a password and social security number) */
    data = write_bytes(file, (char *)addr, ALIGN_UP(bytes, os_vm_page_size),
                       file_offset, core_compression_level);

    write_lispobj(data, file);
    write_lispobj((uword_t)addr, file);
    write_lispobj((bytes + os_vm_page_size - 1) / os_vm_page_size, file);
}

static FILE *
open_core_for_saving(char *filename)
{
    /* Open the output file. We don't actually need the file yet, but
     * the fopen() might fail for some reason, and we want to detect
     * that and back out before we do anything irreversible. */
    unlink(filename);
    return fopen(filename, "wb");
}

void unwind_binding_stack()
{
    bool verbose = !lisp_startup_options.noinform;
    struct thread *th = all_threads;

    /* Smash the enclosing state. (Once we do this, there's no good
     * way to go back, which is a sufficient reason that this ends up
     * being SAVE-LISP-AND-DIE instead of SAVE-LISP-AND-GO-ON). */
    if (verbose) {
        printf("[undoing binding stack and other enclosing state... ");
        fflush(stdout);
    }
    unbind_to_here((lispobj *)th->binding_stack_start,th);
    write_TLS(CURRENT_CATCH_BLOCK, 0, th); // If set to 0 on start, why here too?
    write_TLS(CURRENT_UNWIND_PROTECT_BLOCK, 0, th);
    unsigned int hint = 0;
    char symbol_name[] = "*SAVE-LISP-CLOBBERED-GLOBALS*";
    lispobj* sym = find_symbol(symbol_name,
                               VECTOR(lisp_package_vector)->data[PACKAGE_ID_KERNEL],
                               &hint);
    lispobj value;
    int i;
    if (!sym || !simple_vector_p(value = ((struct symbol*)sym)->value))
        fprintf(stderr, "warning: bad value in %s\n", symbol_name);
    else for(i=vector_len(VECTOR(value))-1; i>=0; --i)
        SYMBOL(VECTOR(value)->data[i])->value = UNBOUND_MARKER_WIDETAG;
    // these are akin to weak-pointers
    lisp_package_vector = 0;
    alloc_profile_data = 0;
    if (verbose) printf("done]\n");
}

bool save_to_filehandle(FILE *file, char *filename, lispobj init_function,
                        bool make_executable,
                        bool save_runtime_options,
                        int core_compression_level)
{
    bool verbose = !lisp_startup_options.noinform;

    /* (Now we can actually start copying ourselves into the output file.) */

    if (verbose) {
        printf("[saving current Lisp image into %s:\n", filename);
        fflush(stdout);
    }

    os_vm_offset_t core_start_pos = FTELL(file);
    write_lispobj(CORE_MAGIC, file);

    /* If 'save_runtime_options' is specified then the saved thread stack size
     * and dynamic space size are used in the restarted image and
     * all command-line arguments are available to Lisp in SB-EXT:*POSIX-ARGV*.
     * Otherwise command-line processing is performed as normal */
    if (save_runtime_options)
        write_memsize_options(file);

    int stringlen = strlen((const char *)build_id);
    int string_words = ALIGN_UP(stringlen, sizeof (core_entry_elt_t))
        / sizeof (core_entry_elt_t);
    int pad = string_words * sizeof (core_entry_elt_t) - stringlen;
    /* Write 3 word entry header: a word for entry-type-code, a word for
     * the total length in words, and a word for the string length */
    write_lispobj(BUILD_ID_CORE_ENTRY_TYPE_CODE, file);
    write_lispobj(3 + string_words, file);
    write_lispobj(stringlen, file);
    int nwrote = fwrite(build_id, 1, stringlen, file);
    /* Write padding bytes to align to core_entry_elt_t */
    while (pad--) nwrote += (fputc(0xff, file) != EOF);
    if (nwrote != (int)(sizeof (core_entry_elt_t) * string_words))
        perror(GENERAL_WRITE_FAILURE_MSG);

    write_lispobj(DIRECTORY_CORE_ENTRY_TYPE_CODE, file);
    write_lispobj(/* (word count = N spaces described by 5 words each, plus the
          * entry type code, plus this count itself) */
         (5 * MAX_CORE_SPACE_ID) + 2, file);
    output_space(file,
                 STATIC_CORE_SPACE_ID,
                 (lispobj *)STATIC_SPACE_START,
                 static_space_free_pointer,
                 core_start_pos,
                 core_compression_level);
#ifdef LISP_FEATURE_DARWIN_JIT
    output_space(file,
                 STATIC_CODE_CORE_SPACE_ID,
                 (lispobj *)STATIC_CODE_SPACE_START,
                 static_code_space_free_pointer,
                 core_start_pos,
                 core_compression_level);
#endif
    output_space(file,
                 DYNAMIC_CORE_SPACE_ID,
                 (void*)DYNAMIC_SPACE_START,
                 (void*)dynamic_space_highwatermark(),
                 core_start_pos,
                 core_compression_level);
    /* FreeBSD really doesn't like to give you the address you asked for
     * on dynamic space. We have a better chance at satisfying the request
     * if we haven't already mapped R/O space below it. */
    output_space(file,
                 READ_ONLY_CORE_SPACE_ID,
                 (lispobj *)READ_ONLY_SPACE_START,
                 read_only_space_free_pointer,
                 core_start_pos,
                 core_compression_level);
#ifdef LISP_FEATURE_IMMOBILE_SPACE
    output_space(file,
                 IMMOBILE_FIXEDOBJ_CORE_SPACE_ID,
                 (lispobj *)FIXEDOBJ_SPACE_START,
                 fixedobj_free_pointer,
                 core_start_pos,
                 core_compression_level);
    // Leave this space for last! Things are easier when splitting a core into
    // code and non-code if we don't have to compensate for removal of pages.
    // i.e. if code resided between dynamic and fixedobj space, then dynamic
    // space would need to have it pages renumbered when code is pulled out.
    output_space(file,
                 IMMOBILE_TEXT_CORE_SPACE_ID,
                 (lispobj *)TEXT_SPACE_START,
                 text_space_highwatermark,
                 core_start_pos,
                 core_compression_level);
#endif

    write_lispobj(INITIAL_FUN_CORE_ENTRY_TYPE_CODE, file);
    write_lispobj(3, file);
    write_lispobj(init_function, file);

#ifdef LISP_FEATURE_GENERATIONAL
    {
        extern void gc_store_corefile_ptes(struct corefile_pte*);
        size_t true_size = next_free_page * sizeof(struct corefile_pte);
        size_t aligned_size = ALIGN_UP(true_size, N_WORD_BYTES);
        char* data = successful_malloc(aligned_size);
        // Zeroize the final few bytes of data that get written out
        // but might be untouched by gc_store_corefile_ptes().
        memset(data + aligned_size - N_WORD_BYTES, 0, N_WORD_BYTES);
        gc_store_corefile_ptes((struct corefile_pte*)data);
        write_lispobj(PAGE_TABLE_CORE_ENTRY_TYPE_CODE, file);
        write_lispobj(6, file); // number of words in this core header entry
        write_lispobj(gc_card_table_nbits, file);
        write_lispobj(next_free_page, file);
        write_lispobj(aligned_size, file);
        sword_t offset = write_bytes(file, data, aligned_size, core_start_pos,
                                     COMPRESSION_LEVEL_NONE);
        write_lispobj(offset, file);
    }
#endif

    write_lispobj(END_CORE_ENTRY_TYPE_CODE, file);

    /* Write a trailing header, ignored when parsing the core normally.
     * This is used to locate the start of the core when the runtime is
     * prepended to it. */
    fseek(file, 0, SEEK_END);

    if (1 != fwrite(&core_start_pos, sizeof(os_vm_offset_t), 1, file)) {
        perror("Error writing core starting position to file");
        fclose(file);
    } else {
        write_lispobj(CORE_MAGIC, file);
        fclose(file);
    }

#ifndef LISP_FEATURE_WIN32
    if (make_executable)
        chmod (filename, 0755);
#endif

    if (verbose) printf("done]\n");
    exit(0);
}

/* Check if the build_id for the current runtime is present in a
 * buffer. */
static int
check_runtime_build_id(void *buf, size_t size)
{
    size_t idlen;
    char *pos;

    idlen = strlen((const char*)build_id) - 1;
    while ((pos = memchr(buf, build_id[0], size)) != NULL) {
        size -= (pos + 1) - (char *)buf;
        buf = (pos + 1);
        if (idlen <= size && memcmp(buf, build_id + 1, idlen) == 0)
            return 1;
    }

    return 0;
}

/* Slurp the executable portion of the runtime into a malloced buffer
 * and return it.  Places the size in bytes of the runtime into
 * 'size_out'.  Returns NULL if the runtime cannot be loaded from
 * 'runtime_path'. */
static void *
load_runtime(char *runtime_path, size_t *size_out)
{
    void *buf = NULL;
    FILE *input = NULL;
    size_t size, count;
    os_vm_offset_t core_offset;

    core_offset = search_for_embedded_core (runtime_path, 0);
    if ((input = fopen(runtime_path, "rb")) == NULL) {
        fprintf(stderr, "Unable to open runtime: %s\n", runtime_path);
        goto lose;
    }

    fseek(input, 0, SEEK_END);
    size = (size_t) ftell(input);
    fseek(input, 0, SEEK_SET);

    if (core_offset != -1 && size > (size_t) core_offset)
        size = core_offset;

    buf = successful_malloc(size);
    if ((count = fread(buf, 1, size, input)) != size) {
        fprintf(stderr, "Premature EOF while reading runtime.\n");
        goto lose;
    }

    if (!check_runtime_build_id(buf, size)) {
        fprintf(stderr, "Failed to locate current build_id in runtime: %s\n",
            runtime_path);
        goto lose;
    }

    fclose(input);
    *size_out = size;
    return buf;

lose:
    if (input != NULL)
        fclose(input);
    if (buf != NULL)
        free(buf);
    return NULL;
}

bool save_runtime_to_filehandle(FILE *output, void *runtime, size_t runtime_size,
                                int __attribute__((unused)) application_type)
{
    size_t padding;
    void *padbytes;

#ifdef LISP_FEATURE_WIN32
    {
        PIMAGE_DOS_HEADER dos_header = (PIMAGE_DOS_HEADER)runtime;
        PIMAGE_NT_HEADERS nt_header = (PIMAGE_NT_HEADERS)((char *)dos_header +
                                                          dos_header->e_lfanew);

        int sub_system;
        switch (application_type) {
        case 0:
            sub_system = IMAGE_SUBSYSTEM_WINDOWS_CUI;
            break;
        case 1:
            sub_system = IMAGE_SUBSYSTEM_WINDOWS_GUI;
            break;
        default:
            fprintf(stderr, "Invalid application type %d\n", application_type);
            return 0;
        }

        nt_header->OptionalHeader.Subsystem = sub_system;
    }
#endif

    if (runtime_size != fwrite(runtime, 1, runtime_size, output)) {
        perror("Error saving runtime");
        return 0;
    }

    padding = (os_vm_page_size - (runtime_size % os_vm_page_size)) & ~os_vm_page_size;
    if (padding > 0) {
        padbytes = successful_malloc(padding);
        memset(padbytes, 0, padding);
        if (padding != fwrite(padbytes, 1, padding, output)) {
            perror("Error saving runtime");
            free(padbytes);
            return 0;
        }
        free(padbytes);
    }

    return 1;
}

FILE *
prepare_to_save(char *filename, bool prepend_runtime, void **runtime_bytes,
                size_t *runtime_size)
{
    FILE *file;
    extern char *sbcl_runtime;

    // SB-IMPL::DEINIT already checked for exactly 1 thread,
    // so this really shouldn't happen.
    if (all_threads->next) {
        fprintf(stderr, "Can't save image with more than one executing thread");
        return NULL;
    }

    if (prepend_runtime) {
        if (!sbcl_runtime) {
            fprintf(stderr, "Unable to get default runtime path.\n");
            return NULL;
        }
        *runtime_bytes = load_runtime(sbcl_runtime, runtime_size);

        if (*runtime_bytes == NULL)
            return 0;
    }

    file = open_core_for_saving(filename);
    if (file == NULL) {
        free(*runtime_bytes);
        perror(filename);
        return NULL;
    }

    return file;
}

#ifdef LISP_FEATURE_CHENEYGC
bool save(char *filename, lispobj init_function, bool prepend_runtime,
          bool save_runtime_options, bool compressed, int compression_level,
          int application_type)
{
    FILE *file;
    void *runtime_bytes = NULL;
    size_t runtime_size;

    file = prepare_to_save(filename, prepend_runtime, &runtime_bytes, &runtime_size);
    if (file == NULL)
        return 1;

    if (prepend_runtime)
        save_runtime_to_filehandle(file, runtime_bytes, runtime_size, application_type);

    /* This unwinding is necessary for proper restoration of the
     * symbol-value slots to their toplevel values, but it occurs
     * too late to remove old references from the binding stack.
     * There's probably no safe way to do that from Lisp */
    unwind_binding_stack();
    os_unlink_runtime();
    return save_to_filehandle(file, filename, init_function, prepend_runtime,
                              save_runtime_options,
                              compressed ? compressed : COMPRESSION_LEVEL_NONE);
}
#endif

/* Things to do before doing a final GC before saving a core.
 *
 * + Single-object pages aren't moved by the GC, so we need to
 *   unset that flag from all pages.
 * + Change all pages' generations to 0 so that we can do all the collection
 *   in a single invocation of collect_generation()
 * + Instances on unboxed pages need to have their layout pointer visited,
 *   so all pages have to be turned to boxed.
 */
static void prepare_dynamic_space_for_final_gc()
{
    page_index_t i;

    prepare_immobile_space_for_final_gc();
    /* TODO: external full compactor thingy for MR */
    for (i = 0; i < next_free_page; i++) {
        // Compaction requires that we permit large objects to be copied henceforth.
        // Object of size >= LARGE_OBJECT_SIZE get re-allocated to single-object pages.
        page_table[i].type &= ~SINGLE_OBJECT_FLAG;
        // Turn every page to boxed so that the layouts of instances
        // which were relocated to unboxed pages get scanned and fixed.
        if ((page_table[i].type & PAGE_TYPE_MASK) == PAGE_TYPE_UNBOXED)
            page_table[i].type = PAGE_TYPE_MIXED;
        generation_index_t gen = page_table[i].gen;
        if (gen != 0) {
            page_table[i].gen = 0;
            int used = page_bytes_used(i);
            generations[gen].bytes_allocated -= used;
            generations[0].bytes_allocated += used;
        }
    }

#ifdef LISP_FEATURE_SB_THREAD
    // Avoid tenuring of otherwise-dead objects referenced by
    // dynamic bindings which disappear on image restart.
    struct thread *thread = get_sb_vm_thread();
    char *start = (char*)&thread->lisp_thread;
    char *end = (char*)thread + dynamic_values_bytes;
    memset(start, 0, end-start);
#endif
    // Make sure that it's done after zeroing above, the GC needs to
    // see a list there
#ifdef PINNED_OBJECTS
    struct thread *th;
    for_each_thread(th) {
        write_TLS(PINNED_OBJECTS, NIL, th);
    }
#endif
}

/* Set this switch to 1 for coalescing of strings dumped to fasl,
 * or 2 for coalescing of those,
 * plus literal strings in code compiled to memory. */
char gc_coalesce_string_literals = 0;

extern void move_rospace_to_dynamic(int), prepare_readonly_space(int,int);

/* Do a non-conservative GC twice, and then save a core with the initial
 * function being set to the value of 'lisp_init_function'.
 * The first GC is into relatively high page indices, and the 2nd is back
 * into lower page indices. This compacts the retained data into the lower
 * pages, minimizing the size of the core file.
 *
 * But note: There is no assurance that this technique actually works,
 * and that the final GC can fit all data below the starting allocation
 * page in the penultimate GC. If it doesn't fit, things are technically
 * ok, but horrible in terms of core file size.  Consider:
 *
 * Penultimate GC: (moves all objects higher in memory)
 *   | ... from_space ... |
 *                        ^--  gencgc_alloc_start_page = next_free_page
 *                        | ... to_space ... |
 *                                           ^ new next_free_page
 *
 * Utimate GC: (moves all objects lower in memory)
 *   | ... to_space ...   | ... from_space ...| ... |
 *                                                  ^ new next_free_page ?
 * Question:
 *  In the ultimate GC, can next_free_page actually increase past
 *  its ending value from the penultimate GC?
 * Answer:
 *  Yes- Suppose the sequence of copying is so adversarial to the allocator
 *  that attempts to fit an object in a region fail often, and require
 *  frequent opening of new regions. (And/or imagine a particularly bad mix
 *  of boxed and non-boxed allocations such that the logic for resuming
 *  at the tail of a partially filled page in gc_find_freeish_pages()
 *  is seldom applicable)  If this occurs, then some allocation must
 *  be on a higher page than all of to_space and from_space.
 *  Then the entire (zeroed) from_space will be present in the saved core
 *  as empty pages, because we can't represent discontiguous ranges.
 */
void
gc_and_save(char *filename, bool prepend_runtime, bool purify,
            bool save_runtime_options, bool compressed,
            int compression_level, int application_type)
{
    // FIXME: Instead of disabling purify for static space relocation,
    // we should make r/o space read-only after fixing up pointers to
    // static space instead.
#if ((defined LISP_FEATURE_SPARC && defined LISP_FEATURE_LINUX) || \
     (defined LISP_FEATURE_RELOCATABLE_STATIC_SPACE))
    /* OS says it'll give you the memory where you want, then it says
     * it won't map over it from the core file.  That's news to me.
     * Fragment of output from 'strace -e mmap2 src/runtime/sbcl --core output/sbcl.core':
     * ...
     * mmap2(0x2fb58000, 4882432, PROT_READ|PROT_WRITE|PROT_EXEC, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0) = 0x2fb58000
     * mmap2(0x2fb58000, 4882432, PROT_READ, MAP_SHARED|MAP_FIXED, 3, 0x2000) = -1 EINVAL (Invalid argument)
     */
    purify = 0;
#endif
    FILE *file;
    void *runtime_bytes = NULL;
    size_t runtime_size;
    extern void coalesce_similar_objects();
    bool verbose = !lisp_startup_options.noinform;

    file = prepare_to_save(filename, prepend_runtime, &runtime_bytes,
                           &runtime_size);
    if (file == NULL)
       return;

    /* The filename might come from Lisp, and be moved by the now
     * non-conservative GC. */
    filename = strdup(filename);

    /* We're committed to process death at this point, and interrupts can not
     * possibly be handled in Lisp. Let the installed handler closures become
     * garbage, since new ones will be made by ENABLE-INTERRUPT on restart */
#ifndef LISP_FEATURE_WIN32
    {
        int i;
        for (i=0; i<NSIG; ++i)
            lisp_sig_handlers[i] = 0;
    }
#endif

    conservative_stack = 0;
    gencgc_oldest_gen_to_gc = 0;
    // From here on until exit, there is no chance of continuing
    // in Lisp if something goes wrong during GC.
    // Flush regions to ensure heap scan in copy_rospace doesn't miss anything
    struct thread *thread = get_sb_vm_thread();
    gc_close_thread_regions(thread, 0);
    gc_close_collector_regions(0);
    move_rospace_to_dynamic(0);
    pre_verify_gen_0 = 1;
    prepare_immobile_space_for_final_gc(); // once is enough
    prepare_dynamic_space_for_final_gc();
    unwind_binding_stack();
    // Avoid tenuring of otherwise-dead objects referenced by bindings which
    // disappear on image restart. This must occcur *after* unwind_binding_stack()
    // because unwinding moves values from the binding stack into TLS.
    char *start = (char*)&thread->lisp_thread;
    char *end = (char*)thread + dynamic_values_bytes;
    memset(start, 0, end-start);
    // After zeroing, make sure PINNED_OBJECTS is a list again.
    write_TLS(PINNED_OBJECTS, NIL, thread);

    save_lisp_gc_iteration = 1;
    collect_garbage(0);
    verify_heap(0, VERIFY_POST_GC);

    THREAD_JIT(0);

    // We always coalesce copyable numbers. Additional coalescing is done
    // only on request, in which case a message is shown (unless verbose=0).
    if (gc_coalesce_string_literals && verbose) {
        printf("[coalescing similar vectors... ");
        fflush(stdout);
    }
    // Now that we've GC'd to eliminate as much junk as possible...
    coalesce_similar_objects();
    if (gc_coalesce_string_literals && verbose)
        printf("done]\n");

    // Do a non-moving collection so that orphaned strings that result
    // from coalescing STRING= symbol names do not consume read-only space.
    collect_garbage(1+PSEUDO_STATIC_GENERATION);
    prepare_readonly_space(purify, 0);
    if (verbose) { printf("[performing final GC..."); fflush(stdout); }
    prepare_dynamic_space_for_final_gc();
    save_lisp_gc_iteration = 2;
    gencgc_alloc_start_page = 0;
    collect_garbage(0);
    /* All global allocation regions should be empty */
    ASSERT_REGIONS_CLOSED();
    // Enforce (rather, warn for lack of) self-containedness of the heap
    verify_heap(0, VERIFY_FINAL | VERIFY_QUICK);
    if (verbose)
        printf(" done]\n");

    THREAD_JIT(0);
    // Scrub remaining garbage
    extern void zero_all_free_ranges(void);
    zero_all_free_ranges();
    // Assert that defrag will not move the init_function
    gc_assert(!immobile_space_p(lisp_init_function));
    // Defragment and set all objects' generations to pseudo-static
    prepare_immobile_space_for_save(verbose);

#ifdef LISP_FEATURE_X86_64
    untune_asm_routines_for_microarch();
#endif
    os_unlink_runtime();

    if (prepend_runtime)
        save_runtime_to_filehandle(file, runtime_bytes, runtime_size,
                                   application_type);

    save_to_filehandle(file, filename, lisp_init_function,
                       prepend_runtime, save_runtime_options,
                       compressed ? compression_level : COMPRESSION_LEVEL_NONE);
    /* Oops. Save still managed to fail. Since we've mangled the stack
     * beyond hope, there's not much we can do.
     * (beyond FUNCALLing lisp_init_function, but I suspect that's
     * going to be rather unsatisfactory too... */
    lose("Attempt to save core after non-conservative GC failed.");
}

/*
 * Fiber support for SBCL - header file.
 *
 * This software is part of the SBCL system.  See the README file for
 * more information.
 */

#ifndef _FIBER_H_
#define _FIBER_H_

#include "genesis/sbcl.h"
#include "runtime.h"
#include "thread.h"
#include <stddef.h>

/* Platform-abstract lock for fiber GC list */
#ifdef LISP_FEATURE_WIN32
#include <windows.h>
typedef CRITICAL_SECTION fiber_lock_t;
#define FIBER_LOCK_INIT(l) InitializeCriticalSection(&(l))
#define FIBER_LOCK(l) EnterCriticalSection(&(l))
#define FIBER_UNLOCK(l) LeaveCriticalSection(&(l))
#else
#include <pthread.h>
typedef pthread_mutex_t fiber_lock_t;
#define FIBER_LOCK_INIT(l) pthread_mutex_init(&(l), NULL)
#define FIBER_LOCK(l) pthread_mutex_lock(&(l))
#define FIBER_UNLOCK(l) pthread_mutex_unlock(&(l))
#endif

/* Fiber stack descriptor */
struct fiber_stack {
    void* base;       /* mmap'd region (low address) */
    size_t size;      /* total size including guard page */
    size_t guard_size; /* size of guard page at bottom */
};

/* GC information for a suspended fiber (doubly-linked list) */
struct fiber_gc_info {
    lispobj* control_stack_base;     /* mmap'd base (low address) */
    lispobj* control_stack_pointer;  /* saved RSP */
    lispobj* control_stack_end;      /* base + size (high address) */
    lispobj* binding_stack_start;
    lispobj* binding_stack_pointer;
    struct fiber_gc_info* next;
    struct fiber_gc_info* prev;
    int registered;                  /* 1 if in the GC list, 0 otherwise */
};

/* Global list of suspended fibers for GC scanning */
extern struct fiber_gc_info* all_fiber_gc_info;
extern fiber_lock_t fiber_gc_lock;

/* Per-carrier-thread context for GC when a fiber is actively running.
 * One entry per carrier thread, linked into a global list.
 * Set by enter_fiber_gc_context() before fiber_switch, cleared by
 * leave_fiber_gc_context() after fiber_switch returns. */
struct active_fiber_context {
    struct thread* carrier_thread;
    lispobj* fiber_stack_start;
    lispobj* fiber_stack_end;
    lispobj* fiber_binding_stack_start;
    struct fiber_gc_info carrier_gc_info;
    struct active_fiber_context* next;
    struct active_fiber_context* prev;
};

/* Find the active fiber context for a given thread (during stop-the-world GC) */
struct active_fiber_context* find_active_fiber_context(struct thread* th);

/* Called from Lisp (via alien-routine) around fiber_switch */
void enter_fiber_gc_context(uword_t fiber_stack_start, uword_t fiber_stack_end,
                            uword_t fiber_bs_start,
                            uword_t carrier_stack_start, uword_t carrier_stack_end,
                            uword_t carrier_bs_start, uword_t carrier_bsp);
void leave_fiber_gc_context(void);

/* Stack allocation/deallocation */
struct fiber_stack* alloc_fiber_stack(size_t size);
void free_fiber_stack(struct fiber_stack* stack);
struct fiber_stack* alloc_fiber_binding_stack(size_t size);

/* GC registration (called from Lisp within without-gcing) */
struct fiber_gc_info* make_fiber_gc_info(void);
void register_fiber_for_gc(struct fiber_gc_info* info);
void unregister_fiber_for_gc(struct fiber_gc_info* info);
void free_fiber_gc_info(struct fiber_gc_info* info);

/* Scan all suspended fiber stacks during GC */
void scan_fiber_stacks(void);
void scavenge_fiber_binding_stacks(int compacting_p, void (*mark_fun)(lispobj));

/* Fiber guard page detection (called from signal handler) */
int check_fiber_guard_page(os_vm_address_t addr);

/* C trampoline called from assembly */
void fiber_run_and_finish(lispobj fiber_lispobj);

#endif /* _FIBER_H_ */

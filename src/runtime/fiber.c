/*
 * Fiber support for SBCL - C runtime.
 *
 * Stack allocation, GC registration, and the fiber_run_and_finish trampoline.
 *
 * This software is part of the SBCL system.  See the README file for
 * more information.
 */

#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>

#include "genesis/sbcl.h"
#include "runtime.h"
#include "os.h"
#include "interr.h"
#include "thread.h"
#include "arch.h"
#include "globals.h"
#include "fiber.h"

/* ===== Global fiber GC list ===== */

struct fiber_gc_info* all_fiber_gc_info = NULL;
fiber_lock_t fiber_gc_lock;
static int fiber_gc_lock_initialized = 0;

static void ensure_fiber_gc_lock(void)
{
    if (!fiber_gc_lock_initialized) {
        FIBER_LOCK_INIT(fiber_gc_lock);
        fiber_gc_lock_initialized = 1;
    }
}

/* ===== Stack allocation ===== */

static size_t round_up_to_page(size_t size)
{
    size_t ps = os_vm_page_size;
    return (size + ps - 1) & ~(ps - 1);
}

struct fiber_stack*
alloc_fiber_stack(size_t size)
{
    struct fiber_stack* stack = malloc(sizeof(struct fiber_stack));
    if (!stack) return NULL;

    size_t ps = os_vm_page_size;
    size_t total = round_up_to_page(size) + ps; /* +1 page for guard */

    os_vm_address_t mem = os_allocate(total);
    if (!mem) {
        free(stack);
        return NULL;
    }

    /* Guard page at the bottom (lowest address) */
    os_protect(mem, ps, OS_VM_PROT_NONE);

    stack->base = mem;
    stack->size = total;
    stack->guard_size = ps;
    return stack;
}

void
free_fiber_stack(struct fiber_stack* stack)
{
    if (stack) {
        os_deallocate(stack->base, stack->size);
        free(stack);
    }
}

struct fiber_stack*
alloc_fiber_binding_stack(size_t size)
{
    /* Binding stacks don't need a guard page - they're bounds-checked */
    struct fiber_stack* stack = malloc(sizeof(struct fiber_stack));
    if (!stack) return NULL;

    size_t total = round_up_to_page(size);

    os_vm_address_t mem = os_allocate(total);
    if (!mem) {
        free(stack);
        return NULL;
    }

    stack->base = mem;
    stack->size = total;
    stack->guard_size = 0;
    return stack;
}

/* ===== GC registration ===== */

struct fiber_gc_info*
make_fiber_gc_info(void)
{
    struct fiber_gc_info* info = malloc(sizeof(struct fiber_gc_info));
    if (info) memset(info, 0, sizeof(*info));
    return info;
}

void
register_fiber_for_gc(struct fiber_gc_info* info)
{
    if (info->registered) return;  /* already in the list */
    ensure_fiber_gc_lock();
    FIBER_LOCK(fiber_gc_lock);
    info->next = all_fiber_gc_info;
    info->prev = NULL;
    if (all_fiber_gc_info)
        all_fiber_gc_info->prev = info;
    all_fiber_gc_info = info;
    info->registered = 1;
    FIBER_UNLOCK(fiber_gc_lock);
}

void
unregister_fiber_for_gc(struct fiber_gc_info* info)
{
    if (!info->registered) return;  /* not in the list */
    ensure_fiber_gc_lock();
    FIBER_LOCK(fiber_gc_lock);
    if (info->prev)
        info->prev->next = info->next;
    else
        all_fiber_gc_info = info->next;
    if (info->next)
        info->next->prev = info->prev;
    info->next = NULL;
    info->prev = NULL;
    info->registered = 0;
    FIBER_UNLOCK(fiber_gc_lock);
}

void
free_fiber_gc_info(struct fiber_gc_info* info)
{
    free(info);
}

/* ===== Per-thread active fiber GC context ===== */

/*
 * When a fiber is running on a carrier thread, RSP and BSP point into
 * the fiber's stacks, not the carrier's.  The GC needs to know:
 *   1. The fiber's stack boundaries (to scan the active fiber's stack)
 *   2. The carrier's suspended stack info (to scan the carrier's stack)
 *
 * Each carrier thread gets its own active_fiber_context, linked into
 * a global list for GC to iterate during stop-the-world.
 */

static struct active_fiber_context* all_active_fiber_contexts = NULL;
static fiber_lock_t active_fiber_context_lock;
static int active_fiber_context_lock_initialized = 0;

/* Per-thread cached context (allocated once, reused across fiber switches) */
#ifdef LISP_FEATURE_GCC_TLS
static __thread struct active_fiber_context* my_active_fiber_context = NULL;
#elif defined(LISP_FEATURE_SB_THREAD) && !defined(LISP_FEATURE_WIN32)
static pthread_key_t active_fiber_context_key;
static int active_fiber_context_key_initialized = 0;
#endif

static void ensure_active_fiber_context_lock(void)
{
    if (!active_fiber_context_lock_initialized) {
        FIBER_LOCK_INIT(active_fiber_context_lock);
        active_fiber_context_lock_initialized = 1;
#if !defined(LISP_FEATURE_GCC_TLS) && defined(LISP_FEATURE_SB_THREAD) && !defined(LISP_FEATURE_WIN32)
        if (!active_fiber_context_key_initialized) {
            pthread_key_create(&active_fiber_context_key, NULL);
            active_fiber_context_key_initialized = 1;
        }
#endif
    }
}

static struct active_fiber_context* get_my_context(void)
{
#ifdef LISP_FEATURE_GCC_TLS
    return my_active_fiber_context;
#elif defined(LISP_FEATURE_SB_THREAD) && !defined(LISP_FEATURE_WIN32)
    return (struct active_fiber_context*)pthread_getspecific(active_fiber_context_key);
#else
    return NULL;
#endif
}

static void set_my_context(struct active_fiber_context* ctx)
{
#ifdef LISP_FEATURE_GCC_TLS
    my_active_fiber_context = ctx;
#elif defined(LISP_FEATURE_SB_THREAD) && !defined(LISP_FEATURE_WIN32)
    pthread_setspecific(active_fiber_context_key, ctx);
#endif
}

static struct active_fiber_context* get_or_create_context(void)
{
    struct active_fiber_context* ctx = get_my_context();
    if (!ctx) {
        ctx = malloc(sizeof(struct active_fiber_context));
        memset(ctx, 0, sizeof(*ctx));
        set_my_context(ctx);
    }
    return ctx;
}

/* Find active fiber context for a thread.  Called during stop-the-world
 * GC, so no lock needed (all mutators stopped). */
struct active_fiber_context*
find_active_fiber_context(struct thread* th)
{
    struct active_fiber_context* ctx;
    for (ctx = all_active_fiber_contexts; ctx; ctx = ctx->next) {
        if (ctx->carrier_thread == th)
            return ctx;
    }
    return NULL;
}

void
enter_fiber_gc_context(uword_t fiber_stack_start, uword_t fiber_stack_end,
                       uword_t fiber_bs_start,
                       uword_t carrier_stack_start, uword_t carrier_stack_end,
                       uword_t carrier_bs_start, uword_t carrier_bsp)
{
    ensure_active_fiber_context_lock();
    struct active_fiber_context* ctx = get_or_create_context();

    ctx->carrier_thread = get_sb_vm_thread();
    ctx->fiber_stack_start = (lispobj*)fiber_stack_start;
    ctx->fiber_stack_end = (lispobj*)fiber_stack_end;
    ctx->fiber_binding_stack_start = (lispobj*)fiber_bs_start;

    /* Register carrier's stack for GC scanning.  The carrier will be
     * suspended by fiber_switch somewhere below our current frame.
     * We use __builtin_frame_address(0) minus a generous 16KB buffer
     * as a conservative approximation of the carrier's eventual
     * suspended RSP, ensuring all frames between here and the actual
     * suspension point are included in the GC's conservative scan.
     * Clamp above the guard pages (3 pages at bottom of stack). */
    ctx->carrier_gc_info.control_stack_base = (lispobj*)carrier_stack_start;
    {
        uintptr_t approx_sp = (uintptr_t)__builtin_frame_address(0) - 16384;
        uintptr_t stack_min = carrier_stack_start + 3 * os_vm_page_size;
        if (approx_sp < stack_min)
            approx_sp = stack_min;
        ctx->carrier_gc_info.control_stack_pointer = (lispobj*)approx_sp;
    }
    ctx->carrier_gc_info.control_stack_end = (lispobj*)carrier_stack_end;
    ctx->carrier_gc_info.binding_stack_start = (lispobj*)carrier_bs_start;
    ctx->carrier_gc_info.binding_stack_pointer = (lispobj*)carrier_bsp;
    register_fiber_for_gc(&ctx->carrier_gc_info);

    /* Link into global list */
    FIBER_LOCK(active_fiber_context_lock);
    ctx->next = all_active_fiber_contexts;
    ctx->prev = NULL;
    if (all_active_fiber_contexts)
        all_active_fiber_contexts->prev = ctx;
    all_active_fiber_contexts = ctx;
    FIBER_UNLOCK(active_fiber_context_lock);
}

void
leave_fiber_gc_context(void)
{
    struct active_fiber_context* ctx = get_my_context();
    if (!ctx) return;

    unregister_fiber_for_gc(&ctx->carrier_gc_info);

    /* Unlink from global list */
    FIBER_LOCK(active_fiber_context_lock);
    if (ctx->prev)
        ctx->prev->next = ctx->next;
    else
        all_active_fiber_contexts = ctx->next;
    if (ctx->next)
        ctx->next->prev = ctx->prev;
    FIBER_UNLOCK(active_fiber_context_lock);

    /* Clear fiber fields but keep ctx allocated for reuse */
    ctx->carrier_thread = NULL;
    ctx->fiber_stack_start = NULL;
    ctx->fiber_stack_end = NULL;
    ctx->fiber_binding_stack_start = NULL;
    ctx->next = NULL;
    ctx->prev = NULL;
}

/* ===== Fiber guard page detection ===== */

int
check_fiber_guard_page(os_vm_address_t addr)
{
    size_t ps = os_vm_page_size;

    /* Check active fiber contexts (fibers currently running on carriers) */
    struct active_fiber_context* ctx;
    for (ctx = all_active_fiber_contexts; ctx; ctx = ctx->next) {
        lispobj* stack_start = ctx->fiber_stack_start;
        if (stack_start) {
            os_vm_address_t guard_start = (os_vm_address_t)stack_start - ps;
            if (addr >= guard_start && addr < (os_vm_address_t)stack_start) {
                lose("Fiber control stack exhausted (fault: %p, fiber stack: %p-%p)",
                     addr, stack_start, ctx->fiber_stack_end);
                return 1;
            }
        }
    }

    /* Check suspended fibers (in the GC list) */
    struct fiber_gc_info* fi;
    for (fi = all_fiber_gc_info; fi; fi = fi->next) {
        lispobj* base = fi->control_stack_base;
        if (base) {
            os_vm_address_t guard_start = (os_vm_address_t)base - ps;
            if (addr >= guard_start && addr < (os_vm_address_t)base) {
                lose("Fiber control stack exhausted (fault: %p, fiber stack: %p-%p)",
                     addr, base, fi->control_stack_end);
                return 1;
            }
        }
    }

    return 0;
}

/* ===== GC scanning of suspended fiber stacks ===== */

/*
 * Called during GC (stop-the-world) to conservatively scan
 * all suspended fiber control stacks.  No lock needed because
 * all mutator threads are stopped.
 */
void
scan_fiber_stacks(void)
{
    /* This function is called from gencgc.c during conservative stack scan.
     * We use preserve_pointer which is declared static in gencgc.c,
     * so the actual scanning loop is inlined there via the scan hook. */
}

/*
 * Called during GC to scavenge fiber binding stacks.
 * The actual call happens from gencgc.c which has access to
 * scav_binding_stack().
 */
void
scavenge_fiber_binding_stacks(__attribute__((unused)) int compacting,
                              __attribute__((unused)) void (*mark_fun)(lispobj))
{
    /* Called from gencgc.c with proper context */
}

/* ===== Fiber run-and-finish trampoline ===== */

/*
 * fiber_run_and_finish(lispobj fiber_lispobj)
 *
 * Called from fiber_entry_trampoline (assembly) when a new fiber
 * starts for the first time.  This function:
 *   1. Extracts the fiber's Lisp function
 *   2. Calls funcall0 on it (entering Lisp)
 *   3. On return (normal or error), marks the fiber dead
 *   4. Unwinds the fiber's dynamic bindings
 *   5. Restores the carrier's BSP
 *   6. Switches back to the scheduler
 *
 * This function must not return (assembly has HLT after call).
 *
 * NOTE: We access the fiber and scheduler structs via genesis-generated
 * C struct definitions.  However, those are generated during the SBCL
 * build and may not be available during initial development.  For now,
 * we use the Lisp-side trampoline approach: the fiber's function is a
 * wrapper that handles all cleanup in Lisp, and this C trampoline
 * simply calls it and does the final context switch.
 *
 * The Lisp wrapper (fiber-trampoline in fiber.lisp) handles:
 *   - Running the user function with error handling
 *   - Marking the fiber dead
 *   - Saving state and unbinding
 *   - Switching back to the scheduler
 *
 * So this C function just needs to call the fiber's function and
 * handle the case where it returns (which shouldn't happen if
 * the Lisp wrapper does its job).
 */
void
fiber_run_and_finish(lispobj fiber_lispobj)
{
    /* The fiber_lispobj is passed from assembly.  The Lisp side
     * set up the fiber's function slot to be a closure that wraps
     * the user's function with proper cleanup.  That closure
     * calls fiber_switch back to the scheduler when done.
     * So we just need to call it.
     *
     * Access the function slot: fiber is an instance, and the
     * function slot is at a known offset.  We use the Lisp-callable
     * entry point FIBER-RUN-AND-FINISH-LISP which takes the fiber
     * as an argument.
     *
     * For simplicity, we invoke this through a static Lisp function
     * that the Lisp side registers.
     */

    /* Call the registered Lisp-side fiber runner.
     * This is set up by (setf fiber-c-trampoline-fun ...) in fiber.lisp.
     * It calls the fiber's function, handles errors, cleans up bindings,
     * and switches back to the scheduler.  It does NOT return. */
    extern lispobj fiber_run_trampoline;
    if (fiber_run_trampoline) {
        funcall1(fiber_run_trampoline, fiber_lispobj);
    }

    /* Should not reach here */
    lose("fiber_run_and_finish: Lisp trampoline returned!");
}

/* Lisp-callable trampoline function, set from Lisp side */
lispobj fiber_run_trampoline = 0;

/* Return the address of fiber_entry_trampoline (for Lisp to read) */
uword_t
get_fiber_entry_trampoline_addr(void)
{
#if defined(LISP_FEATURE_X86_64) || defined(LISP_FEATURE_ARM64) || defined(LISP_FEATURE_ARM)
    return (uword_t)fiber_entry_trampoline;
#else
    return 0;  /* Fibers not yet supported on this architecture */
#endif
}

/* Set the Lisp trampoline function (called from Lisp) */
void
set_fiber_run_trampoline(lispobj fun)
{
    fiber_run_trampoline = fun;
}

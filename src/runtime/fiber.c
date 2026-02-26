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

/* ===== Active fiber GC context ===== */

/*
 * When a fiber is running on a carrier thread, RSP and BSP point into
 * the fiber's stacks, not the carrier's.  The GC needs to know:
 *   1. The fiber's stack boundaries (to scan the active fiber's stack)
 *   2. The carrier's suspended stack info (to scan the carrier's stack)
 *
 * These globals are set before fiber_switch (entering a fiber) and
 * cleared after fiber_switch returns (leaving a fiber).
 */

lispobj* gc_active_fiber_stack_start = NULL;
lispobj* gc_active_fiber_stack_end = NULL;
lispobj* gc_active_fiber_binding_stack_start = NULL;

/* gc_info for the carrier's suspended state while a fiber runs */
static struct fiber_gc_info gc_carrier_stack_info;

void
enter_fiber_gc_context(uword_t fiber_stack_start, uword_t fiber_stack_end,
                       uword_t fiber_bs_start,
                       uword_t carrier_stack_start, uword_t carrier_stack_end,
                       uword_t carrier_bs_start, uword_t carrier_bsp)
{
    gc_active_fiber_stack_start = (lispobj*)fiber_stack_start;
    gc_active_fiber_stack_end = (lispobj*)fiber_stack_end;
    gc_active_fiber_binding_stack_start = (lispobj*)fiber_bs_start;

    /* Register carrier's binding stack for GC scavenging.
     * Set control_stack_pointer = control_stack_end so the control stack
     * scan loop in gencgc.c sees an empty range (ptr < end is immediately
     * false).  The carrier's control stack is scanned directly by the
     * fiber-aware code in gencgc.c which skips the guard pages. */
    gc_carrier_stack_info.control_stack_base = (lispobj*)carrier_stack_start;
    gc_carrier_stack_info.control_stack_pointer = (lispobj*)carrier_stack_end;
    gc_carrier_stack_info.control_stack_end = (lispobj*)carrier_stack_end;
    gc_carrier_stack_info.binding_stack_start = (lispobj*)carrier_bs_start;
    gc_carrier_stack_info.binding_stack_pointer = (lispobj*)carrier_bsp;
    register_fiber_for_gc(&gc_carrier_stack_info);
}

void
leave_fiber_gc_context(void)
{
    unregister_fiber_for_gc(&gc_carrier_stack_info);
    gc_active_fiber_stack_start = NULL;
    gc_active_fiber_stack_end = NULL;
    gc_active_fiber_binding_stack_start = NULL;
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

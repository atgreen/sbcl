/*
 * Fiber context switch assembly for ARM64 (AArch64).
 *
 * This software is part of the SBCL system.  See the README file for
 * more information.
 *
 * This software is derived from the CMU CL system, which was
 * written at Carnegie Mellon University and released into the
 * public domain.  The software is in the public domain and is
 * provided with absolutely no warranty.  See the COPYING and CREDITS
 * files for more information.
 */

#ifdef __ELF__
/* Mark stack as non-executable for GNU ld */
.section .note.GNU-stack,"",@progbits
#endif

/*
 * Portability macros — match arm64-assem.S conventions for
 * Darwin / Win32 / ELF symbol decoration and type/size directives.
 */
#ifdef LISP_FEATURE_DARWIN
#define TYPE(name)
#define SIZE(name)
#define GNAME(var) _##var
#elif defined(LISP_FEATURE_WIN32)
#define TYPE(name)
#define SIZE(name)
#define GNAME(var) var
#else
#define TYPE(name) .type name,%function
#define SIZE(name) .size name,.-name
#define GNAME(var) var
#endif

.text

/*
 * fiber_switch(void* old_sp_slot, void* new_sp_slot, void* thread_ptr)
 *
 * AAPCS64 calling convention:
 *   Arguments: x0, x1, x2
 *   Callee-saved: x19-x28, x29(FP), x30(LR), d8-d15
 *
 * thread_ptr (3rd arg): On ARM64 with sb-thread, x21 holds the thread
 * pointer.  fiber_switch saves/restores x21, so when a fiber migrates
 * between carriers, the restored x21 would point to the old carrier's
 * thread struct.  If thread_ptr is non-zero, x21 is set to thread_ptr
 * after restoring registers, ensuring the correct thread context.
 *
 * Save 20 callee-saved registers (160 bytes) onto the current stack,
 * store SP into *old_sp_slot, load SP from *new_sp_slot, restore
 * registers, and return.
 *
 * Stack layout after saves (lowest address at top, sp points here):
 *   SP+0x00  x29 (frame pointer)
 *   SP+0x08  x30 (link register / return address)
 *   SP+0x10  x19
 *   SP+0x18  x20
 *   SP+0x20  x21
 *   SP+0x28  x22
 *   SP+0x30  x23
 *   SP+0x38  x24
 *   SP+0x40  x25
 *   SP+0x48  x26
 *   SP+0x50  x27
 *   SP+0x58  x28
 *   SP+0x60  d8
 *   SP+0x68  d9
 *   SP+0x70  d10
 *   SP+0x78  d11
 *   SP+0x80  d12
 *   SP+0x88  d13
 *   SP+0x90  d14
 *   SP+0x98  d15
 */
        .globl GNAME(fiber_switch)
        TYPE(GNAME(fiber_switch))
        .balign 16
GNAME(fiber_switch):
        /* Save callee-saved registers (160 bytes) */
        stp     x29, x30, [sp, #-160]!
        stp     x19, x20, [sp, #16]
        stp     x21, x22, [sp, #32]
        stp     x23, x24, [sp, #48]
        stp     x25, x26, [sp, #64]
        stp     x27, x28, [sp, #80]
        stp     d8,  d9,  [sp, #96]
        stp     d10, d11, [sp, #112]
        stp     d12, d13, [sp, #128]
        stp     d14, d15, [sp, #144]

        /* Save current SP to *old_sp_slot */
        mov     x2, sp
        str     x2, [x0]

        /* Load new SP from *new_sp_slot */
        ldr     x2, [x1]
        mov     sp, x2

        /* Restore callee-saved registers */
        ldp     d14, d15, [sp, #144]
        ldp     d12, d13, [sp, #128]
        ldp     d10, d11, [sp, #112]
        ldp     d8,  d9,  [sp, #96]
        ldp     x27, x28, [sp, #80]
        ldp     x25, x26, [sp, #64]
        ldp     x23, x24, [sp, #48]
        ldp     x21, x22, [sp, #32]
        ldp     x19, x20, [sp, #16]
        ldp     x29, x30, [sp], #160

        /* Patch thread register if thread_ptr (x2) is non-zero */
        cbz     x2, 1f
        mov     x21, x2
1:      ret
        SIZE(GNAME(fiber_switch))

/*
 * fiber_entry_trampoline -- initial return target for a brand-new fiber.
 *
 * When fiber_switch "returns" into a new fiber for the first time,
 * it lands here via x30 (loaded from the initial stack frame).
 * The fiber lispobj is in x29, loaded by fiber_switch's
 * "ldp x29, x30" from the initial stack frame.
 *
 * Move fiber lispobj to x0 (first C argument), clear x29,
 * and call fiber_run_and_finish.
 */
        .globl GNAME(fiber_entry_trampoline)
        TYPE(GNAME(fiber_entry_trampoline))
        .balign 16
GNAME(fiber_entry_trampoline):
        mov     x0, x29             /* fiber lispobj -> first arg */
        mov     x29, xzr            /* clear frame pointer */
        bl      GNAME(fiber_run_and_finish)
        brk     #0                  /* unreachable */
        SIZE(GNAME(fiber_entry_trampoline))

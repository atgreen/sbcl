/*
 * Fiber context switch assembly for x86-64.
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

.text

/*
 * fiber_switch(uint64_t* old_rsp_slot, uint64_t* new_rsp_slot,
 *              uint64_t thread_ptr)
 *
 * Saves callee-saved registers onto the current stack, stores RSP into
 * *old_rsp_slot, loads RSP from *new_rsp_slot, pops registers, and returns.
 *
 * thread_ptr (3rd arg): On x86-64 without gs-seg, r13 holds the thread
 * pointer.  fiber_switch saves/restores r13, so when a fiber migrates
 * between carriers, the restored r13 would point to the old carrier's
 * thread struct.  If thread_ptr is non-zero, r13 is set to thread_ptr
 * after restoring registers, ensuring the correct thread context.
 */

#ifdef _WIN32
/*
 * Windows x64 ABI:
 *   Arguments: rcx, rdx, r8 (not rdi, rsi, rdx)
 *   Callee-saved: rbx, rbp, rdi, rsi, r12-r15
 *
 * Stack layout after saves (lowest address at top):
 *   RSP+0x00  r15
 *   RSP+0x08  r14
 *   RSP+0x10  r13
 *   RSP+0x18  r12
 *   RSP+0x20  rsi
 *   RSP+0x28  rdi
 *   RSP+0x30  rbx
 *   RSP+0x38  rbp
 *   RSP+0x40  return address  (placed by caller's CALL instruction)
 */
.globl fiber_switch
.balign 16
fiber_switch:
        pushq   %rbp
        pushq   %rbx
        pushq   %rdi
        pushq   %rsi
        pushq   %r12
        pushq   %r13
        pushq   %r14
        pushq   %r15
        movq    %rsp, (%rcx)       /* *old_rsp_slot = rsp */
        movq    (%rdx), %rsp       /* rsp = *new_rsp_slot */
        popq    %r15
        popq    %r14
        popq    %r13
        popq    %r12
        popq    %rsi
        popq    %rdi
        popq    %rbx
        popq    %rbp
        /* Patch thread register if thread_ptr (r8) is non-zero */
        testq   %r8, %r8
        jz      1f
        movq    %r8, %r13
1:      ret

/*
 * fiber_entry_trampoline -- initial return target for a brand-new fiber.
 *
 * Windows x64: fiber lispobj is in RBP (popped by fiber_switch).
 * Move to RCX (first arg), clear RBP, allocate 32-byte shadow space.
 */
.globl fiber_entry_trampoline
.balign 16
fiber_entry_trampoline:
        movq    %rbp, %rcx          /* fiber lispobj -> first arg (rcx) */
        xorl    %ebp, %ebp          /* clear frame pointer */
        subq    $32, %rsp           /* shadow space for Win64 ABI */
        call    fiber_run_and_finish
        hlt                         /* unreachable */

#else /* SysV ABI (Linux, macOS, FreeBSD, etc.) */

/*
 * SysV x86-64 ABI:
 *   Arguments: rdi, rsi, rdx
 *   Callee-saved: rbx, rbp, r12-r15
 *
 * Stack layout after saves (lowest address at top):
 *   RSP+0x00  r15
 *   RSP+0x08  r14
 *   RSP+0x10  r13
 *   RSP+0x18  r12
 *   RSP+0x20  rbx
 *   RSP+0x28  rbp
 *   RSP+0x30  return address  (placed by caller's CALL instruction)
 *
 * The new_rsp_slot must point to a stack with the same layout.
 */
.globl fiber_switch
.type  fiber_switch, @function
.balign 16
fiber_switch:
        pushq   %rbp
        pushq   %rbx
        pushq   %r12
        pushq   %r13
        pushq   %r14
        pushq   %r15
        movq    %rsp, (%rdi)       /* *old_rsp_slot = rsp */
        movq    (%rsi), %rsp       /* rsp = *new_rsp_slot */
        popq    %r15
        popq    %r14
        popq    %r13
        popq    %r12
        popq    %rbx
        popq    %rbp
        /* Patch thread register if thread_ptr (rdx) is non-zero */
        testq   %rdx, %rdx
        jz      1f
        movq    %rdx, %r13
1:      ret
.size fiber_switch, .-fiber_switch

/*
 * fiber_entry_trampoline -- initial return target for a brand-new fiber.
 *
 * When fiber_switch "returns" into a new fiber for the first time, it
 * lands here.  At this point RSP points to stack_top - 0x10 (16-byte
 * aligned).  The fiber lispobj is in RBP, loaded by fiber_switch's
 * "popq %rbp" from the stack slot at stack_top - 0x20.  The trampoline
 * moves it to RDI (first C argument register) and zeros RBP before
 * calling fiber_run_and_finish.
 *
 * fiber_run_and_finish(fiber_lispobj) enters Lisp, runs the fiber's
 * function, marks it dead, and switches back to the scheduler.
 * It must not return.
 */
.globl fiber_entry_trampoline
.type  fiber_entry_trampoline, @function
.balign 16
fiber_entry_trampoline:
        movq    %rbp, %rdi          /* fiber lispobj was loaded into rbp */
        xorl    %ebp, %ebp          /* clear frame pointer */
        call    fiber_run_and_finish
        hlt                         /* unreachable */
.size fiber_entry_trampoline, .-fiber_entry_trampoline

#endif /* _WIN32 */

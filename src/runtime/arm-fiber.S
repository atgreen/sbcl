/*
 * Fiber context switch assembly for ARM (32-bit).
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
        .fpu vfp

/*
 * fiber_switch(void* old_sp_slot, void* new_sp_slot, void* thread_ptr)
 *
 * AAPCS calling convention:
 *   Arguments: r0, r1, r2
 *   Callee-saved: r4-r11, lr, d8-d15
 *
 * thread_ptr (3rd arg): ignored on ARM32 (no threading support).
 *
 * Save callee-saved registers onto the current stack, store SP into
 * *old_sp_slot, load SP from *new_sp_slot, restore registers, return.
 *
 * Stack layout after saves (lowest address at top, sp points here):
 *   SP+0x00..0x3F  d8-d15 (8 doubles x 8 bytes = 64 bytes)
 *   SP+0x40        r3  (padding for 8-byte alignment)
 *   SP+0x44        r4
 *   SP+0x48        r5
 *   SP+0x4C        r6
 *   SP+0x50        r7
 *   SP+0x54        r8
 *   SP+0x58        r9
 *   SP+0x5C        r10
 *   SP+0x60        r11
 *   SP+0x64        lr
 *
 * Total frame: 104 bytes (64 VFP + 40 integer)
 */
        .global fiber_switch
        .type   fiber_switch, %function
        .balign 4
fiber_switch:
        /* Save integer callee-saved regs + lr (r3 as padding) */
        stmfd   sp!, {r3-r11, lr}       @ 10 regs x 4 = 40 bytes
        /* Save VFP callee-saved regs */
        vstmdb  sp!, {d8-d15}           @ 8 doubles x 8 = 64 bytes

        /* Save current SP to *old_sp_slot */
        str     sp, [r0]

        /* Load new SP from *new_sp_slot */
        ldr     sp, [r1]

        /* Restore VFP callee-saved regs */
        vldmia  sp!, {d8-d15}
        /* Restore integer callee-saved regs + lr */
        ldmfd   sp!, {r3-r11, lr}

        bx      lr
        .size   fiber_switch, .-fiber_switch

/*
 * fiber_entry_trampoline -- initial return target for a brand-new fiber.
 *
 * When fiber_switch "returns" into a new fiber for the first time,
 * it lands here via lr (loaded from the initial stack frame).
 * The fiber lispobj is in r11, loaded by fiber_switch's ldmfd from
 * the initial stack frame.
 *
 * Move fiber lispobj to r0 (first C argument), clear r11,
 * and call fiber_run_and_finish.
 */
        .global fiber_entry_trampoline
        .type   fiber_entry_trampoline, %function
        .balign 4
fiber_entry_trampoline:
        mov     r0, r11             @ fiber lispobj -> first arg
        mov     r11, #0             @ clear frame pointer
        bl      fiber_run_and_finish
        .inst   0xe7f000f0          @ UDF (undefined instruction trap, unreachable)
        .size   fiber_entry_trampoline, .-fiber_entry_trampoline

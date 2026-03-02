;;;; Fiber context-switch assembly routines for ARM (32-bit).
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap SP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; AAPCS arguments:
;;;;   r0 – raw address of old saved-SP word
;;;;   r1 – raw address of new saved-SP word
;;;;   r2 – thread pointer (ignored on ARM32, no SB-THREAD)
;;;;
;;;; Saves d8-d15 (64 bytes) + r3-r11,lr (40 bytes) = 104 bytes.
;;;; Stack layout (SP points to base after saves):
;;;;   SP+0x00..0x3F  d8-d15 (VFP callee-saved, 8 doubles)
;;;;   SP+0x40        r3 (padding)
;;;;   SP+0x44        r4
;;;;   SP+0x48        r5
;;;;   SP+0x4C        r6
;;;;   SP+0x50        r7
;;;;   SP+0x54        r8
;;;;   SP+0x58        r9
;;;;   SP+0x5C        r10
;;;;   SP+0x60        r11
;;;;   SP+0x64        lr

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg r0-offset)    ; r0
     (:temp new-slot unsigned-reg r1-offset)    ; r1
     ;; r2 (thread-ptr) is unused on ARM32
     ;; Callee-saved GPRs needing :temp declarations
     (:temp r3-save any-reg lexenv-offset)      ; r3
     (:temp r4-save any-reg nl2-offset)         ; r4
     (:temp r6-save any-reg nl3-offset)         ; r6
     (:temp r8-save any-reg r8-offset)          ; r8
     (:temp r9-save any-reg nfp-offset)         ; r9
     ;; VFP callee-saved: d8 start TN for fstmd/fldmd
     (:temp d8 double-reg 16))                  ; d8 = float-reg offset 16

  ;; Allocate 104-byte frame
  (inst sub nsp-tn nsp-tn 104)

  ;; Save VFP d8-d15 at [SP+0..56] (8 double regs, unindexed mode)
  (inst fstmd nsp-tn d8 8)

  ;; Save integer callee-saved at [SP+64..100]
  (inst str r3-save (@ nsp-tn 64))             ; r3
  (inst str r4-save (@ nsp-tn 68))             ; r4
  (inst str code-tn (@ nsp-tn 72))             ; r5
  (inst str r6-save (@ nsp-tn 76))             ; r6
  (inst str ocfp-tn (@ nsp-tn 80))             ; r7
  (inst str r8-save (@ nsp-tn 84))             ; r8
  (inst str r9-save (@ nsp-tn 88))             ; r9
  (inst str null-tn (@ nsp-tn 92))             ; r10
  (inst str cfp-tn (@ nsp-tn 96))              ; r11
  (inst str lr-tn (@ nsp-tn 100))              ; lr

  ;; *old-slot = SP
  (inst str nsp-tn (@ old-slot))

  ;; SP = *new-slot
  (inst ldr nsp-tn (@ new-slot))

  ;; Restore integer callee-saved from [SP+64..100]
  (inst ldr r3-save (@ nsp-tn 64))             ; r3
  (inst ldr r4-save (@ nsp-tn 68))             ; r4
  (inst ldr code-tn (@ nsp-tn 72))             ; r5
  (inst ldr r6-save (@ nsp-tn 76))             ; r6
  (inst ldr ocfp-tn (@ nsp-tn 80))             ; r7
  (inst ldr r8-save (@ nsp-tn 84))             ; r8
  (inst ldr r9-save (@ nsp-tn 88))             ; r9
  (inst ldr null-tn (@ nsp-tn 92))             ; r10
  (inst ldr cfp-tn (@ nsp-tn 96))              ; r11
  (inst ldr lr-tn (@ nsp-tn 100))              ; lr

  ;; Restore VFP d8-d15 from [SP+0..56]
  (inst fldmd nsp-tn d8 8)

  ;; Deallocate frame
  (inst add nsp-tn nsp-tn 104)

  (inst bx lr-tn))

;;;; ----------------------------------------------------------------
;;;; FIBER-ENTRY-TRAMPOLINE
;;;;
;;;; Initial return target for brand-new fibers.  fiber-switch's
;;;; restore loads the fiber lispobj into r11 (cfp-tn) and this
;;;; routine's address into lr from the initial stack frame.
;;;; The "bx lr" then jumps here.

#+sb-assembling
(define-assembly-routine (fiber-entry-trampoline (:return-style :none))
    ((:temp arg0 unsigned-reg r0-offset))  ; r0 for C arg

  ;; r11 (cfp-tn) = fiber lispobj (from fiber-switch's restore)
  (inst mov arg0 cfp-tn)                ; fiber lispobj -> first C arg
  (inst mov cfp-tn 0)                   ; clear frame pointer
  ;; Call fiber_run_and_finish via literal pool
  (let ((fixup (gen-label)))
    (inst load-from-label pc-tn lr-tn fixup)
    (assemble (:elsewhere)
      (emit-label fixup)
      (inst word (make-fixup "fiber_run_and_finish" :foreign))))
  ;; Unreachable -- emit undefined instruction trap
  (inst word #xe7f000f0))

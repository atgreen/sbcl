;;;; Fiber context-switch assembly routines for PPC64 (ELFv2 ABI).
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap SP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; ELFv2 ABI arguments (via custom VOP):
;;;;   r3 (nl0) – raw address of old saved-SP word
;;;;   r4 (nl1) – raw address of new saved-SP word
;;;;   r5 (nl2) – thread pointer (patch r30 if non-zero)
;;;;
;;;; Saves LR, CR, r14-r31 (18 GPRs), f14-f31 (18 FPRs).
;;;; Frame: 320 bytes (16-byte aligned).
;;;;
;;;; Layout from SP:
;;;;   +0:   backchain (old SP, stored by stdu)
;;;;   +8:   saved LR
;;;;   +16:  saved CR
;;;;   +24:  r14 (bsp) .. +160: r31 (lip)    [18 GPRs × 8]
;;;;   +168: f14 .. +304: f31                 [18 FPRs × 8]
;;;;   +312..+319: padding to 320

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg nl0-offset)    ; r3
     (:temp new-slot unsigned-reg nl1-offset)    ; r4
     (:temp thread-ptr unsigned-reg nl2-offset)  ; r5
     (:temp temp unsigned-reg nl3-offset)        ; r6 scratch
     ;; GPRs without predefined TNs
     (:temp nfp-save any-reg nfp-offset)         ; r20
     (:temp a0-save descriptor-reg a0-offset)    ; r24
     (:temp a1-save descriptor-reg a1-offset)    ; r25
     (:temp a2-save descriptor-reg a2-offset)    ; r26
     (:temp a3-save descriptor-reg a3-offset)    ; r27
     (:temp l0-save descriptor-reg l0-offset)    ; r28
     (:temp l1-save descriptor-reg l1-offset)    ; r29
     ;; Callee-saved FPRs f14-f31
     (:temp f14 double-reg 14) (:temp f15 double-reg 15)
     (:temp f16 double-reg 16) (:temp f17 double-reg 17)
     (:temp f18 double-reg 18) (:temp f19 double-reg 19)
     (:temp f20 double-reg 20) (:temp f21 double-reg 21)
     (:temp f22 double-reg 22) (:temp f23 double-reg 23)
     (:temp f24 double-reg 24) (:temp f25 double-reg 25)
     (:temp f26 double-reg 26) (:temp f27 double-reg 27)
     (:temp f28 double-reg 28) (:temp f29 double-reg 29)
     (:temp f30 double-reg 30) (:temp f31 double-reg 31))

  ;; Allocate 320-byte frame; stdu stores backchain at [new SP+0]
  (inst stdu nsp-tn nsp-tn -320)

  ;; Save LR and CR
  (inst mflr temp)
  (inst std temp nsp-tn 8)
  (inst mfcr temp)
  (inst std temp nsp-tn 16)

  ;; Save callee-saved GPRs r14-r31
  (inst std bsp-tn nsp-tn 24)            ; r14
  (inst std cfp-tn nsp-tn 32)            ; r15
  (inst std csp-tn nsp-tn 40)            ; r16
  (inst std card-table-base-tn nsp-tn 48) ; r17
  (inst std null-tn nsp-tn 56)           ; r18
  (inst std code-tn nsp-tn 64)           ; r19
  (inst std nfp-save nsp-tn 72)          ; r20
  (inst std lexenv-tn nsp-tn 80)         ; r21
  (inst std ocfp-tn nsp-tn 88)           ; r22
  (inst std lra-tn nsp-tn 96)            ; r23
  (inst std a0-save nsp-tn 104)          ; r24
  (inst std a1-save nsp-tn 112)          ; r25
  (inst std a2-save nsp-tn 120)          ; r26
  (inst std a3-save nsp-tn 128)          ; r27
  (inst std l0-save nsp-tn 136)          ; r28
  (inst std l1-save nsp-tn 144)          ; r29
  (inst std thread-base-tn nsp-tn 152)   ; r30
  (inst std lip-tn nsp-tn 160)           ; r31

  ;; Save callee-saved FPRs f14-f31
  (inst stfd f14 nsp-tn 168)
  (inst stfd f15 nsp-tn 176)
  (inst stfd f16 nsp-tn 184)
  (inst stfd f17 nsp-tn 192)
  (inst stfd f18 nsp-tn 200)
  (inst stfd f19 nsp-tn 208)
  (inst stfd f20 nsp-tn 216)
  (inst stfd f21 nsp-tn 224)
  (inst stfd f22 nsp-tn 232)
  (inst stfd f23 nsp-tn 240)
  (inst stfd f24 nsp-tn 248)
  (inst stfd f25 nsp-tn 256)
  (inst stfd f26 nsp-tn 264)
  (inst stfd f27 nsp-tn 272)
  (inst stfd f28 nsp-tn 280)
  (inst stfd f29 nsp-tn 288)
  (inst stfd f30 nsp-tn 296)
  (inst stfd f31 nsp-tn 304)

  ;; *old-slot = SP
  (inst std nsp-tn old-slot 0)
  ;; SP = *new-slot
  (inst ld nsp-tn new-slot 0)

  ;; Restore callee-saved FPRs f14-f31
  (inst lfd f14 nsp-tn 168)
  (inst lfd f15 nsp-tn 176)
  (inst lfd f16 nsp-tn 184)
  (inst lfd f17 nsp-tn 192)
  (inst lfd f18 nsp-tn 200)
  (inst lfd f19 nsp-tn 208)
  (inst lfd f20 nsp-tn 216)
  (inst lfd f21 nsp-tn 224)
  (inst lfd f22 nsp-tn 232)
  (inst lfd f23 nsp-tn 240)
  (inst lfd f24 nsp-tn 248)
  (inst lfd f25 nsp-tn 256)
  (inst lfd f26 nsp-tn 264)
  (inst lfd f27 nsp-tn 272)
  (inst lfd f28 nsp-tn 280)
  (inst lfd f29 nsp-tn 288)
  (inst lfd f30 nsp-tn 296)
  (inst lfd f31 nsp-tn 304)

  ;; Restore callee-saved GPRs r14-r31
  (inst ld bsp-tn nsp-tn 24)             ; r14
  (inst ld cfp-tn nsp-tn 32)             ; r15
  (inst ld csp-tn nsp-tn 40)             ; r16
  (inst ld card-table-base-tn nsp-tn 48) ; r17
  (inst ld null-tn nsp-tn 56)            ; r18
  (inst ld code-tn nsp-tn 64)            ; r19
  (inst ld nfp-save nsp-tn 72)           ; r20
  (inst ld lexenv-tn nsp-tn 80)          ; r21
  (inst ld ocfp-tn nsp-tn 88)            ; r22
  (inst ld lra-tn nsp-tn 96)             ; r23
  (inst ld a0-save nsp-tn 104)           ; r24
  (inst ld a1-save nsp-tn 112)           ; r25
  (inst ld a2-save nsp-tn 120)           ; r26
  (inst ld a3-save nsp-tn 128)           ; r27
  (inst ld l0-save nsp-tn 136)           ; r28
  (inst ld l1-save nsp-tn 144)           ; r29
  (inst ld thread-base-tn nsp-tn 152)    ; r30
  (inst ld lip-tn nsp-tn 160)            ; r31

  ;; Restore CR and LR
  (inst ld temp nsp-tn 16)
  (inst mtcrf #xff temp)
  (inst ld temp nsp-tn 8)
  (inst mtlr temp)

  ;; Deallocate frame
  (inst addi nsp-tn nsp-tn 320)

  ;; Patch thread register if thread-ptr is non-zero.
  ;; When a fiber migrates between carriers, the restored r30 would be
  ;; stale; the caller passes the current carrier's thread pointer.
  (inst cmpdi thread-ptr 0)
  (inst beq DONE)
  (inst mr thread-base-tn thread-ptr)
  DONE
  (inst blr))

;;;; ----------------------------------------------------------------
;;;; FIBER-ENTRY-TRAMPOLINE
;;;;
;;;; Initial return target for brand-new fibers.  fiber-switch's
;;;; register restore loads the fiber lispobj into cfp-tn (r15) and
;;;; the trampoline address into LR from the initial stack frame.
;;;; The "blr" then jumps here.
;;;;
;;;; We move the fiber lispobj into the first C argument register,
;;;; clear CFP, and tail-call fiber_run_and_finish.

#+sb-assembling
(define-assembly-routine (fiber-entry-trampoline (:return-style :none))
    ((:temp arg0 unsigned-reg nl0-offset)       ; r3 (first C arg)
     (:temp cfunc-reg unsigned-reg cfunc-offset)) ; r12 (ELFv2 entry point)

  ;; cfp-tn (r15) = fiber lispobj (from fiber-switch's restore)
  (inst mr arg0 cfp-tn)                  ; fiber lispobj -> r3
  (inst li cfp-tn 0)                     ; clear frame pointer
  ;; Call fiber_run_and_finish.  ELFv2 requires r12 = function address
  ;; at the global entry point so the callee can set up its own TOC.
  (inst lr cfunc-reg (make-fixup "fiber_run_and_finish" :foreign))
  (inst mtctr cfunc-reg)
  (inst bctr)                             ; tail-call, never returns
  (inst unimp 0))                         ; unreachable trap

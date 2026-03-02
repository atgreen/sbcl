;;;; Fiber context-switch assembly routines for PPC (32-bit).
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap SP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; PPC32 arguments (via custom VOP):
;;;;   r3 (nl0) – raw address of old saved-SP word
;;;;   r4 (nl1) – raw address of new saved-SP word
;;;;   r5 (nl2) – thread pointer (patch r17 if non-zero)
;;;;
;;;; Saves LR, CR, r14-r31 (18 GPRs), f14-f31 (18 FPRs).
;;;; Frame: 240 bytes (16-byte aligned).
;;;;
;;;; Layout from SP:
;;;;   +0:   backchain (4 bytes, stored by stwu)
;;;;   +4:   saved LR (4 bytes)
;;;;   +8:   saved CR (4 bytes)
;;;;   +12:  r14 (bsp) .. +80: r31 (lip)    [18 GPRs × 4 = 72 bytes]
;;;;   +84:  padding (4 bytes, for FPR alignment)
;;;;   +88:  f14 .. +224: f31               [18 FPRs × 8 = 144 bytes]
;;;;   +232..+239: padding to 240

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg nl0-offset)    ; r3
     (:temp new-slot unsigned-reg nl1-offset)    ; r4
     (:temp thread-ptr unsigned-reg nl2-offset)  ; r5
     (:temp temp unsigned-reg nl3-offset)        ; r6 scratch
     ;; GPRs without predefined TNs
     (:temp cname-save any-reg cname-offset)     ; r20
     (:temp a0-save descriptor-reg a0-offset)    ; r24
     (:temp a1-save descriptor-reg a1-offset)    ; r25
     (:temp a2-save descriptor-reg a2-offset)    ; r26
     (:temp a3-save descriptor-reg a3-offset)    ; r27
     (:temp l0-save descriptor-reg l0-offset)    ; r28
     (:temp l1-save descriptor-reg l1-offset)    ; r29
     (:temp l2-save descriptor-reg l2-offset)    ; r30
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

  ;; Allocate 240-byte frame; stwu stores backchain at [new SP+0]
  (inst stwu nsp-tn nsp-tn -240)

  ;; Save LR and CR
  (inst mflr temp)
  (inst stw temp nsp-tn 4)
  (inst mfcr temp)
  (inst stw temp nsp-tn 8)

  ;; Save callee-saved GPRs r14-r31 (4 bytes each)
  (inst stw bsp-tn nsp-tn 12)            ; r14
  (inst stw cfp-tn nsp-tn 16)            ; r15
  (inst stw csp-tn nsp-tn 20)            ; r16
  (inst stw thread-base-tn nsp-tn 24)    ; r17
  (inst stw null-tn nsp-tn 28)           ; r18
  (inst stw code-tn nsp-tn 32)           ; r19
  (inst stw cname-save nsp-tn 36)        ; r20
  (inst stw lexenv-tn nsp-tn 40)         ; r21
  (inst stw ocfp-tn nsp-tn 44)           ; r22
  (inst stw lra-tn nsp-tn 48)            ; r23
  (inst stw a0-save nsp-tn 52)           ; r24
  (inst stw a1-save nsp-tn 56)           ; r25
  (inst stw a2-save nsp-tn 60)           ; r26
  (inst stw a3-save nsp-tn 64)           ; r27
  (inst stw l0-save nsp-tn 68)           ; r28
  (inst stw l1-save nsp-tn 72)           ; r29
  (inst stw l2-save nsp-tn 76)           ; r30
  (inst stw lip-tn nsp-tn 80)            ; r31

  ;; Save callee-saved FPRs f14-f31 (8 bytes each, starting at +88)
  (inst stfd f14 nsp-tn 88)
  (inst stfd f15 nsp-tn 96)
  (inst stfd f16 nsp-tn 104)
  (inst stfd f17 nsp-tn 112)
  (inst stfd f18 nsp-tn 120)
  (inst stfd f19 nsp-tn 128)
  (inst stfd f20 nsp-tn 136)
  (inst stfd f21 nsp-tn 144)
  (inst stfd f22 nsp-tn 152)
  (inst stfd f23 nsp-tn 160)
  (inst stfd f24 nsp-tn 168)
  (inst stfd f25 nsp-tn 176)
  (inst stfd f26 nsp-tn 184)
  (inst stfd f27 nsp-tn 192)
  (inst stfd f28 nsp-tn 200)
  (inst stfd f29 nsp-tn 208)
  (inst stfd f30 nsp-tn 216)
  (inst stfd f31 nsp-tn 224)

  ;; *old-slot = SP
  (inst stw nsp-tn old-slot 0)
  ;; SP = *new-slot
  (inst lwz nsp-tn new-slot 0)

  ;; Restore callee-saved FPRs f14-f31
  (inst lfd f14 nsp-tn 88)
  (inst lfd f15 nsp-tn 96)
  (inst lfd f16 nsp-tn 104)
  (inst lfd f17 nsp-tn 112)
  (inst lfd f18 nsp-tn 120)
  (inst lfd f19 nsp-tn 128)
  (inst lfd f20 nsp-tn 136)
  (inst lfd f21 nsp-tn 144)
  (inst lfd f22 nsp-tn 152)
  (inst lfd f23 nsp-tn 160)
  (inst lfd f24 nsp-tn 168)
  (inst lfd f25 nsp-tn 176)
  (inst lfd f26 nsp-tn 184)
  (inst lfd f27 nsp-tn 192)
  (inst lfd f28 nsp-tn 200)
  (inst lfd f29 nsp-tn 208)
  (inst lfd f30 nsp-tn 216)
  (inst lfd f31 nsp-tn 224)

  ;; Restore callee-saved GPRs r14-r31
  (inst lwz bsp-tn nsp-tn 12)            ; r14
  (inst lwz cfp-tn nsp-tn 16)            ; r15
  (inst lwz csp-tn nsp-tn 20)            ; r16
  (inst lwz thread-base-tn nsp-tn 24)    ; r17
  (inst lwz null-tn nsp-tn 28)           ; r18
  (inst lwz code-tn nsp-tn 32)           ; r19
  (inst lwz cname-save nsp-tn 36)        ; r20
  (inst lwz lexenv-tn nsp-tn 40)         ; r21
  (inst lwz ocfp-tn nsp-tn 44)           ; r22
  (inst lwz lra-tn nsp-tn 48)            ; r23
  (inst lwz a0-save nsp-tn 52)           ; r24
  (inst lwz a1-save nsp-tn 56)           ; r25
  (inst lwz a2-save nsp-tn 60)           ; r26
  (inst lwz a3-save nsp-tn 64)           ; r27
  (inst lwz l0-save nsp-tn 68)           ; r28
  (inst lwz l1-save nsp-tn 72)           ; r29
  (inst lwz l2-save nsp-tn 76)           ; r30
  (inst lwz lip-tn nsp-tn 80)            ; r31

  ;; Restore CR and LR
  (inst lwz temp nsp-tn 8)
  (inst mtcrf #xff temp)
  (inst lwz temp nsp-tn 4)
  (inst mtlr temp)

  ;; Deallocate frame
  (inst addi nsp-tn nsp-tn 240)

  ;; Patch thread register if thread-ptr is non-zero.
  ;; When a fiber migrates between carriers, the restored r17 would be
  ;; stale; the caller passes the current carrier's thread pointer.
  (inst cmpwi thread-ptr 0)
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
    ((:temp arg0 unsigned-reg nl0-offset)        ; r3 (first C arg)
     (:temp cfunc-reg unsigned-reg cfunc-offset)) ; r12 or r13 (platform)

  ;; cfp-tn (r15) = fiber lispobj (from fiber-switch's restore)
  (inst mr arg0 cfp-tn)                  ; fiber lispobj -> r3
  (inst li cfp-tn 0)                     ; clear frame pointer
  ;; Call fiber_run_and_finish.  Load address into cfunc register
  ;; and tail-call through CTR.
  (inst lr cfunc-reg (make-fixup "fiber_run_and_finish" :foreign))
  (inst mtctr cfunc-reg)
  (inst bctr)                             ; tail-call, never returns
  (inst unimp 0))                         ; unreachable trap

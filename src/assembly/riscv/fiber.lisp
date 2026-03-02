;;;; Fiber context-switch assembly routines for RISC-V (64-bit).
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap SP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; RISC-V arguments (via custom VOP):
;;;;   x11 (nl0) – raw address of old saved-SP word
;;;;   x13 (nl1) – raw address of new saved-SP word
;;;;   x15 (nl2) – thread pointer (patch x26 if non-zero)
;;;;
;;;; Saves ra, cfp (for debugger), s0-s11 (12 GPRs), fs0-fs11 (12 FPRs).
;;;; Frame: 208 bytes (16-byte aligned).
;;;;
;;;; Layout from SP:
;;;;   +0:   ra/x1       (8 bytes)
;;;;   +8:   cfp/x6      (8 bytes — not callee-saved, saved for debugger)
;;;;   +16:  nfp/s0/x8   (8 bytes)
;;;;   +24:  csp/s1/x9   (8 bytes)
;;;;   +32:  a4/s2/x18   (8 bytes)
;;;;   +40:  nl4/s3/x19  (8 bytes)
;;;;   +48:  a5/s4/x20   (8 bytes)
;;;;   +56:  nl5/s5/x21  (8 bytes)
;;;;   +64:  l0/s6/x22   (8 bytes)
;;;;   +72:  nl6/s7/x23  (8 bytes)
;;;;   +80:  l1/s8/x24   (8 bytes)
;;;;   +88:  tmp/s9/x25  (8 bytes)
;;;;   +96:  thread/s10  (8 bytes)
;;;;   +104: cfunc/s11   (8 bytes)
;;;;   +112: fs0..fs11   [12 FPRs × 8 = 96 bytes]
;;;;   +208: end

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg nl0-offset)     ; x11
     (:temp new-slot unsigned-reg nl1-offset)     ; x13
     (:temp thread-ptr unsigned-reg nl2-offset)   ; x15
     ;; Callee-saved GPRs without predefined TNs
     (:temp a4-save descriptor-reg a4-offset)     ; x18 (s2)
     (:temp nl4-save non-descriptor-reg nl4-offset) ; x19 (s3)
     (:temp a5-save descriptor-reg a5-offset)     ; x20 (s4)
     (:temp nl5-save non-descriptor-reg nl5-offset) ; x21 (s5)
     (:temp l0-save descriptor-reg l0-offset)     ; x22 (s6)
     (:temp nl6-save non-descriptor-reg nl6-offset) ; x23 (s7)
     (:temp l1-save descriptor-reg l1-offset)     ; x24 (s8)
     (:temp cfunc-save non-descriptor-reg cfunc-offset) ; x27 (s11)
     ;; Callee-saved FPRs fs0-fs11
     (:temp fs0 double-reg 8) (:temp fs1 double-reg 9)
     (:temp fs2 double-reg 18) (:temp fs3 double-reg 19)
     (:temp fs4 double-reg 20) (:temp fs5 double-reg 21)
     (:temp fs6 double-reg 22) (:temp fs7 double-reg 23)
     (:temp fs8 double-reg 24) (:temp fs9 double-reg 25)
     (:temp fs10 double-reg 26) (:temp fs11 double-reg 27))

  ;; Allocate 208-byte frame
  (inst subi nsp-tn nsp-tn 208)

  ;; Save GPRs (14 registers × 8 bytes)
  (inst sd ra-tn nsp-tn 0)               ; ra (x1)
  (inst sd cfp-tn nsp-tn 8)              ; cfp (x6, for debugger)
  (inst sd nfp-tn nsp-tn 16)             ; nfp/s0 (x8)
  (inst sd csp-tn nsp-tn 24)             ; csp/s1 (x9)
  (inst sd a4-save nsp-tn 32)            ; a4/s2 (x18)
  (inst sd nl4-save nsp-tn 40)           ; nl4/s3 (x19)
  (inst sd a5-save nsp-tn 48)            ; a5/s4 (x20)
  (inst sd nl5-save nsp-tn 56)           ; nl5/s5 (x21)
  (inst sd l0-save nsp-tn 64)            ; l0/s6 (x22)
  (inst sd nl6-save nsp-tn 72)           ; nl6/s7 (x23)
  (inst sd l1-save nsp-tn 80)            ; l1/s8 (x24)
  (inst sd tmp-tn nsp-tn 88)             ; tmp/s9 (x25)
  (inst sd thread-base-tn nsp-tn 96)     ; thread/s10 (x26)
  (inst sd cfunc-save nsp-tn 104)        ; cfunc/s11 (x27)

  ;; Save callee-saved FPRs fs0-fs11 (12 × 8 = 96 bytes, starting at +112)
  (inst fstore :double fs0 nsp-tn 112)   ; f8
  (inst fstore :double fs1 nsp-tn 120)   ; f9
  (inst fstore :double fs2 nsp-tn 128)   ; f18
  (inst fstore :double fs3 nsp-tn 136)   ; f19
  (inst fstore :double fs4 nsp-tn 144)   ; f20
  (inst fstore :double fs5 nsp-tn 152)   ; f21
  (inst fstore :double fs6 nsp-tn 160)   ; f22
  (inst fstore :double fs7 nsp-tn 168)   ; f23
  (inst fstore :double fs8 nsp-tn 176)   ; f24
  (inst fstore :double fs9 nsp-tn 184)   ; f25
  (inst fstore :double fs10 nsp-tn 192)  ; f26
  (inst fstore :double fs11 nsp-tn 200)  ; f27

  ;; *old-slot = SP
  (inst sd nsp-tn old-slot 0)
  ;; SP = *new-slot
  (inst ld nsp-tn new-slot 0)

  ;; Restore callee-saved FPRs fs0-fs11
  (inst fload :double fs0 nsp-tn 112)
  (inst fload :double fs1 nsp-tn 120)
  (inst fload :double fs2 nsp-tn 128)
  (inst fload :double fs3 nsp-tn 136)
  (inst fload :double fs4 nsp-tn 144)
  (inst fload :double fs5 nsp-tn 152)
  (inst fload :double fs6 nsp-tn 160)
  (inst fload :double fs7 nsp-tn 168)
  (inst fload :double fs8 nsp-tn 176)
  (inst fload :double fs9 nsp-tn 184)
  (inst fload :double fs10 nsp-tn 192)
  (inst fload :double fs11 nsp-tn 200)

  ;; Restore GPRs
  (inst ld ra-tn nsp-tn 0)
  (inst ld cfp-tn nsp-tn 8)
  (inst ld nfp-tn nsp-tn 16)
  (inst ld csp-tn nsp-tn 24)
  (inst ld a4-save nsp-tn 32)
  (inst ld nl4-save nsp-tn 40)
  (inst ld a5-save nsp-tn 48)
  (inst ld nl5-save nsp-tn 56)
  (inst ld l0-save nsp-tn 64)
  (inst ld nl6-save nsp-tn 72)
  (inst ld l1-save nsp-tn 80)
  (inst ld tmp-tn nsp-tn 88)
  (inst ld thread-base-tn nsp-tn 96)
  (inst ld cfunc-save nsp-tn 104)

  ;; Deallocate frame
  (inst addi nsp-tn nsp-tn 208)

  ;; Patch thread register if thread-ptr is non-zero.
  ;; When a fiber migrates between carriers, the restored x26 would be
  ;; stale; the caller passes the current carrier's thread pointer.
  (inst beq thread-ptr zero-tn DONE)
  (move thread-base-tn thread-ptr)
  DONE
  (inst jalr zero-tn ra-tn 0))           ; ret

;;;; ----------------------------------------------------------------
;;;; FIBER-ENTRY-TRAMPOLINE
;;;;
;;;; Initial return target for brand-new fibers.  fiber-switch's
;;;; register restore loads the fiber lispobj into cfp-tn (x6) and
;;;; the trampoline address into ra from the initial stack frame.
;;;; The "ret" then jumps here.
;;;;
;;;; We move the fiber lispobj into the first C argument register (x10),
;;;; clear CFP, and tail-call fiber_run_and_finish.

#+sb-assembling
(define-assembly-routine (fiber-entry-trampoline (:return-style :none))
    ((:temp arg0 any-reg a0-offset)                ; x10 (first C arg = ca0)
     (:temp cfunc-reg non-descriptor-reg cfunc-offset)) ; x27 scratch

  ;; cfp-tn (x6) = fiber lispobj (from fiber-switch's restore)
  (move arg0 cfp-tn)                     ; fiber lispobj -> x10
  (move cfp-tn zero-tn)                  ; clear frame pointer
  ;; Tail-call fiber_run_and_finish
  (inst li cfunc-reg (make-fixup "fiber_run_and_finish" :foreign))
  (inst jalr zero-tn cfunc-reg 0)        ; tail-call, never returns
  (inst ebreak 0))                        ; unreachable trap

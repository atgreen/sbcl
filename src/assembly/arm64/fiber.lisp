;;;; Fiber context-switch assembly routines for ARM64 (AArch64).
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap SP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; AAPCS64 arguments:
;;;;   x0 (nl0) – raw address of old saved-SP word
;;;;   x1 (nl1) – raw address of new saved-SP word
;;;;   x2 (nl2) – thread pointer (patch x21 if non-zero)
;;;;
;;;; Saves x19-x28, x29, x30, d8-d15 (160 bytes).

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg nl0-offset)     ; x0
     (:temp new-slot unsigned-reg nl1-offset)     ; x1
     (:temp thread-ptr unsigned-reg nl2-offset)   ; x2
     (:temp temp unsigned-reg nl3-offset)         ; x3 scratch
     ;; Callee-saved GPRs without predefined TNs
     (:temp x19 unsigned-reg 19)                  ; r9
     (:temp x20 unsigned-reg 20)                  ; r10
     (:temp x24 unsigned-reg 24)                  ; nfp
     ;; Callee-saved FP registers
     (:temp d8  double-reg 8)
     (:temp d9  double-reg 9)
     (:temp d10 double-reg 10)
     (:temp d11 double-reg 11)
     (:temp d12 double-reg 12)
     (:temp d13 double-reg 13)
     (:temp d14 double-reg 14)
     (:temp d15 double-reg 15))

  ;; Save callee-saved registers (160 bytes)
  ;; Layout: [x29,x30, x19,x20, x21,x22, x23,x24, x25,x26, x27,x28,
  ;;          d8,d9, d10,d11, d12,d13, d14,d15]
  (inst stp cfp-tn lr-tn (@ nsp-tn -160 :pre-index))
  (inst stp x19 x20 (@ nsp-tn 16))
  (inst stp thread-tn lexenv-tn (@ nsp-tn 32))
  (inst stp nargs-tn x24 (@ nsp-tn 48))
  (inst stp ocfp-tn null-tn (@ nsp-tn 64))
  (inst stp csp-tn cardtable-tn (@ nsp-tn 80))
  (inst stp d8 d9 (@ nsp-tn 96))
  (inst stp d10 d11 (@ nsp-tn 112))
  (inst stp d12 d13 (@ nsp-tn 128))
  (inst stp d14 d15 (@ nsp-tn 144))

  ;; *old-slot = SP
  (inst mov-sp temp nsp-tn)
  (inst str temp (@ old-slot))

  ;; SP = *new-slot
  (inst ldr temp (@ new-slot))
  (inst mov-sp nsp-tn temp)

  ;; Restore callee-saved registers
  (inst ldp d14 d15 (@ nsp-tn 144))
  (inst ldp d12 d13 (@ nsp-tn 128))
  (inst ldp d10 d11 (@ nsp-tn 112))
  (inst ldp d8 d9 (@ nsp-tn 96))
  (inst ldp csp-tn cardtable-tn (@ nsp-tn 80))
  (inst ldp ocfp-tn null-tn (@ nsp-tn 64))
  (inst ldp nargs-tn x24 (@ nsp-tn 48))
  (inst ldp thread-tn lexenv-tn (@ nsp-tn 32))
  (inst ldp x19 x20 (@ nsp-tn 16))
  (inst ldp cfp-tn lr-tn (@ nsp-tn 160 :post-index))

  ;; Patch thread register if thread-ptr is non-zero.
  ;; When a fiber migrates between carriers, the restored x21 would be
  ;; stale; the caller passes the current carrier's thread pointer.
  (inst cbz thread-ptr DONE)
  (inst mov thread-tn thread-ptr)
  DONE
  (inst ret))

;;;; ----------------------------------------------------------------
;;;; FIBER-ENTRY-TRAMPOLINE
;;;;
;;;; Initial return target for brand-new fibers.  fiber-switch's
;;;; "ldp x29, x30" loads the fiber lispobj into x29 and this
;;;; routine's address into x30.  The "ret" then jumps here.

#+sb-assembling
(define-assembly-routine (fiber-entry-trampoline (:return-style :none))
    ((:temp arg0 unsigned-reg nl0-offset)   ; x0
     (:temp tmp-reg unsigned-reg nl3-offset)) ; x3 scratch for foreign call
  ;; x29 (cfp-tn) = fiber lispobj (from fiber-switch's ldp)
  (inst mov arg0 cfp-tn)               ; fiber lispobj -> first C arg
  (inst mov cfp-tn zr-tn)              ; clear frame pointer
  (invoke-foreign-routine "fiber_run_and_finish" tmp-reg)
  (inst brk 0))                         ; unreachable

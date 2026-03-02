;;;; VOP for fiber context switch on RISC-V.

(in-package "SB-VM")

;;; %FIBER-SWITCH VOP
;;;
;;; Moves three unboxed word arguments into x11/x13/x15 (nl0/nl1/nl2)
;;; and calls the FIBER-SWITCH assembly routine.

(define-vop (%fiber-switch)
  (:translate sb-thread::%fiber-switch)
  (:policy :fast-safe)
  (:args (old-slot :scs (unsigned-reg) :target old)
         (new-slot :scs (unsigned-reg) :target new)
         (thread-ptr :scs (unsigned-reg) :target tp))
  (:arg-types unsigned-num unsigned-num unsigned-num)
  (:temporary (:sc unsigned-reg :offset nl0-offset
               :from (:argument 0))
              old)
  (:temporary (:sc unsigned-reg :offset nl1-offset
               :from (:argument 1))
              new)
  (:temporary (:sc unsigned-reg :offset nl2-offset
               :from (:argument 2))
              tp)
  (:temporary (:sc non-descriptor-reg) jump)
  (:generator 0
    (move old old-slot)
    (move new new-slot)
    (move tp thread-ptr)
    ;; Call fiber-switch assembly routine
    (inst li jump (make-fixup 'fiber-switch :assembly-routine))
    (inst jalr ra-tn jump 0)))

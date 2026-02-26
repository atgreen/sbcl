;;;; VOP for fiber context switch on PPC64.

(in-package "SB-VM")

;;; %FIBER-SWITCH VOP
;;;
;;; Moves three unboxed word arguments into r3/r4/r5 (ELFv2) and
;;; calls the FIBER-SWITCH assembly routine via LR.

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
  (:temporary (:sc any-reg) jump)
  (:generator 0
    (move old old-slot)
    (move new new-slot)
    (move tp thread-ptr)
    ;; Call fiber-switch assembly routine via LR
    (inst addi jump null-tn (make-fixup 'fiber-switch :assembly-routine))
    (inst mtlr jump)
    (inst blrl)))

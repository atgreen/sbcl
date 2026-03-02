;;;; VOP for fiber context switch on ARM (32-bit).

(in-package "SB-VM")

;;; %FIBER-SWITCH VOP
;;;
;;; Moves three unboxed word arguments into r0/r1/r2 (AAPCS) and
;;; calls the FIBER-SWITCH assembly routine via BLX.

(define-vop (%fiber-switch)
  (:translate sb-thread::%fiber-switch)
  (:policy :fast-safe)
  (:args (old-slot :scs (unsigned-reg) :target old)
         (new-slot :scs (unsigned-reg) :target new)
         (thread-ptr :scs (unsigned-reg) :target tp))
  (:arg-types unsigned-num unsigned-num unsigned-num)
  (:temporary (:sc unsigned-reg :offset r0-offset
               :from (:argument 0))
              old)
  (:temporary (:sc unsigned-reg :offset r1-offset
               :from (:argument 1))
              new)
  (:temporary (:sc unsigned-reg :offset r2-offset
               :from (:argument 2))
              tp)
  (:temporary (:sc interior-reg) lip)
  (:temporary (:sc non-descriptor-reg) tmp)
  (:generator 0
    (move old old-slot)
    (move new new-slot)
    (move tp thread-ptr)
    ;; Call fiber-switch assembly routine via literal pool + BLX
    (let ((fixup (gen-label)))
      (assemble (:elsewhere)
        (emit-label fixup)
        (inst word (make-fixup 'fiber-switch :assembly-routine)))
      (inst load-from-label tmp lip fixup)
      (inst blx tmp))))

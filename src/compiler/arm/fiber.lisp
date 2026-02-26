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
  (:generator 0
    (move old old-slot)
    (move new new-slot)
    (move tp thread-ptr)
    ;; Call fiber-switch assembly routine via literal pool + BLX
    (let ((fixup (gen-label)))
      (inst load-from-label lr-tn lr-tn fixup)
      (inst blx lr-tn)
      (assemble (:elsewhere vop)
        (emit-label fixup)
        (inst word (make-fixup 'fiber-switch :assembly-routine))))))

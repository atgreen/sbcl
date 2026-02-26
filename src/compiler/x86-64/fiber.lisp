;;;; VOP for fiber context switch on x86-64.

(in-package "SB-VM")

;;; %FIBER-SWITCH VOP
;;;
;;; Moves three unboxed word arguments into the correct ABI registers
;;; and calls the FIBER-SWITCH assembly routine.  Using a custom VOP
;;; (rather than the auto-generated one from define-assembly-routine)
;;; ensures proper fixnum → word unboxing via :arg-types unsigned-num.

(define-vop (%fiber-switch)
  (:translate sb-thread::%fiber-switch)
  (:policy :fast-safe)
  (:args (old-slot :scs (unsigned-reg) :target old)
         (new-slot :scs (unsigned-reg) :target new)
         (thread-ptr :scs (unsigned-reg) :target tp))
  (:arg-types unsigned-num unsigned-num unsigned-num)
  (:temporary (:sc unsigned-reg
               :offset #-win32 rdi-offset #+win32 rcx-offset
               :from (:argument 0))
              old)
  (:temporary (:sc unsigned-reg
               :offset #-win32 rsi-offset #+win32 rdx-offset
               :from (:argument 1))
              new)
  (:temporary (:sc unsigned-reg
               :offset #-win32 rdx-offset #+win32 r8-offset
               :from (:argument 2))
              tp)
  (:generator 0
    (move old old-slot)
    (move new new-slot)
    (move tp thread-ptr)
    (inst call (make-fixup 'fiber-switch :assembly-routine))))

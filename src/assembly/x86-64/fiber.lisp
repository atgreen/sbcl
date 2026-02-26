;;;; Fiber context-switch assembly routines for x86-64.
;;;;
;;;; These replace the hand-written x86-64-fiber.S, using SBCL's Lisp
;;;; assembler (define-assembly-routine) so that the code is
;;;; cross-platform, hackable from the REPL, and free of OS-specific
;;;; symbol decoration.
;;;;
;;;; Two routines:
;;;;   FIBER-SWITCH         – save/restore callee-saved regs, swap RSP
;;;;   FIBER-ENTRY-TRAMPOLINE – initial landing pad for brand-new fibers

(in-package "SB-VM")

;;;; ----------------------------------------------------------------
;;;; FIBER-SWITCH
;;;;
;;;; Called from the %FIBER-SWITCH VOP.  Arguments arrive in ABI
;;;; registers (SysV: RDI/RSI/RDX, Win64: RCX/RDX/R8):
;;;;   old-slot  – raw address of the saved-RSP word to write
;;;;   new-slot  – raw address of the saved-RSP word to read
;;;;   thread-ptr – if non-zero, patch R13 (thread register) after restore
;;;;
;;;; :return-style :none means the auto-generated VOP is a tail-call
;;;; (we don't use it; a custom VOP does CALL instead).

#+sb-assembling
(define-assembly-routine (fiber-switch (:return-style :none))
    ((:temp old-slot unsigned-reg #-win32 rdi-offset #+win32 rcx-offset)
     (:temp new-slot unsigned-reg #-win32 rsi-offset #+win32 rdx-offset)
     (:temp thread-ptr unsigned-reg #-win32 rdx-offset #+win32 r8-offset))

  ;; Save callee-saved registers onto the current stack.
  ;; SysV: rbp rbx r12-r15  (6 regs, 48 bytes)
  ;; Win64: rbp rbx rdi rsi r12-r15  (8 regs, 64 bytes)
  (inst push rbp-tn)
  (inst push rbx-tn)
  #+win32 (inst push rdi-tn)
  #+win32 (inst push rsi-tn)
  (inst push r12-tn)
  (inst push r13-tn)
  (inst push r14-tn)
  (inst push r15-tn)

  ;; *old-slot = RSP
  (inst mov (ea old-slot) rsp-tn)
  ;; RSP = *new-slot
  (inst mov rsp-tn (ea new-slot))

  ;; Restore callee-saved registers (reverse order).
  (inst pop r15-tn)
  (inst pop r14-tn)
  (inst pop r13-tn)
  (inst pop r12-tn)
  #+win32 (inst pop rsi-tn)
  #+win32 (inst pop rdi-tn)
  (inst pop rbx-tn)
  (inst pop rbp-tn)

  ;; Patch thread register if thread-ptr is non-zero.
  ;; On x86-64 without :gs-seg, r13 holds the thread pointer; when a
  ;; fiber migrates between carriers the restored r13 would be stale.
  (inst test thread-ptr thread-ptr)
  (inst jmp :z DONE)
  (inst mov r13-tn thread-ptr)
  DONE
  (inst ret))

;;;; ----------------------------------------------------------------
;;;; FIBER-ENTRY-TRAMPOLINE
;;;;
;;;; Initial return target for a brand-new fiber.  fiber-switch's
;;;; "pop rbp" loads the fiber lispobj (placed on the initial stack by
;;;; initialize-fiber-stack) into RBP.  The "ret" then jumps here.
;;;;
;;;; We move the fiber lispobj into the first C argument register,
;;;; clear RBP, and call fiber_run_and_finish (C function).

#+sb-assembling
(define-assembly-routine (fiber-entry-trampoline (:return-style :none)) ()
  ;; RBP = fiber lispobj (from fiber-switch's pop)
  #-win32 (inst mov rdi-tn rbp-tn)
  #+win32 (inst mov rcx-tn rbp-tn)
  (inst xor rbp-tn rbp-tn)             ; clear frame pointer
  #+win32 (inst sub rsp-tn 32)         ; shadow space for Win64 ABI
  (inst call (make-fixup "fiber_run_and_finish" :foreign))
  (inst hlt))                           ; unreachable

;;;; Fiber (virtual thread / green thread) support for SBCL.
;;;;
;;;; Provides lightweight cooperative threads that multiplex onto
;;;; carrier OS threads.  Each fiber gets its own mmap'd control stack
;;;; and binding stack.  Context switching saves/restores callee-saved
;;;; registers via assembly (fiber_switch in x86-64-fiber.S).
;;;;
;;;; This software is part of the SBCL system.  See the README file
;;;; for more information.

(in-package "SB-THREAD")

;;;; ===== Foreign function declarations =====

(define-alien-routine "alloc_fiber_stack"
    system-area-pointer (size unsigned-long))

(define-alien-routine "free_fiber_stack"
    void (stack system-area-pointer))

(define-alien-routine "alloc_fiber_binding_stack"
    system-area-pointer (size unsigned-long))

(define-alien-routine "make_fiber_gc_info"
    system-area-pointer)

(define-alien-routine "register_fiber_for_gc"
    void (info system-area-pointer))

(define-alien-routine "unregister_fiber_for_gc"
    void (info system-area-pointer))

(define-alien-routine "free_fiber_gc_info"
    void (info system-area-pointer))


(define-alien-routine "enter_fiber_gc_context"
    void
  (fiber-stack-start unsigned-long)
  (fiber-stack-end unsigned-long)
  (fiber-bs-start unsigned-long)
  (carrier-stack-start unsigned-long)
  (carrier-stack-end unsigned-long)
  (carrier-bs-start unsigned-long)
  (carrier-bsp unsigned-long))

(define-alien-routine "leave_fiber_gc_context"
    void)

;;;; ===== Constants =====

(defconstant +default-fiber-stack-size+ (* 256 1024))  ; 256KB
(defconstant +default-fiber-binding-stack-size+ (* 16 1024)) ; 16KB

;;;; ===== Work-Stealing Deque (Chase-Lev) =====
;;;
;;; Each carrier owns a deque: push/pop from the bottom (LIFO, good locality),
;;; thieves steal from the top (FIFO, breadth-first).  Lock-free via CAS on
;;; the top index.  Buffer is a circular power-of-2 array that grows as needed.
;;;
;;; top and bottom are typed T (holding fixnums) so that COMPARE-AND-SWAP
;;; works on them via the standard SBCL CAS infrastructure.

(defstruct (work-stealing-deque (:constructor %make-wsd)
                                (:conc-name wsd-))
  (buffer (make-array 64 :initial-element nil) :type simple-vector)
  (bottom 0 :type t)
  (top 0 :type t))

(defun wsd-grow (deque)
  "Double the buffer size, copying existing entries."
  (let* ((old-buf (wsd-buffer deque))
         (old-size (length old-buf))
         (new-size (* old-size 2))
         (new-buf (make-array new-size :initial-element nil))
         (top (the fixnum (wsd-top deque)))
         (bottom (the fixnum (wsd-bottom deque)))
         (old-mask (1- old-size))
         (new-mask (1- new-size)))
    (loop for i from top below bottom
          do (setf (svref new-buf (logand i new-mask))
                   (svref old-buf (logand i old-mask))))
    (setf (wsd-buffer deque) new-buf)))

(defun wsd-push (deque fiber)
  "Owner pushes FIBER to the bottom of DEQUE."
  (let* ((b (the fixnum (wsd-bottom deque)))
         (t-val (the fixnum (wsd-top deque)))
         (buf (wsd-buffer deque))
         (size (length buf)))
    ;; Grow if full
    (when (>= (- b t-val) size)
      (wsd-grow deque)
      (setf buf (wsd-buffer deque)))
    (setf (svref buf (logand b (1- (length buf)))) fiber)
    (sb-thread:barrier (:write))
    (setf (wsd-bottom deque) (1+ b))))

(defun wsd-pop (deque)
  "Owner pops from the bottom of DEQUE. Returns NIL if empty."
  (let* ((b (1- (the fixnum (wsd-bottom deque)))))
    (setf (wsd-bottom deque) b)
    (sb-thread:barrier (:memory))
    (let ((t-val (the fixnum (wsd-top deque))))
      (cond
        ((> b t-val)
         ;; More than one element; safe to take
         (svref (wsd-buffer deque) (logand b (1- (length (wsd-buffer deque))))))
        ((= b t-val)
         ;; Last element; race with stealers
         (let ((item (svref (wsd-buffer deque) (logand b (1- (length (wsd-buffer deque)))))))
           (if (eq (sb-ext:cas (wsd-top deque) t-val (1+ t-val)) t-val)
               (progn (setf (wsd-bottom deque) (1+ t-val)) item)
               (progn (setf (wsd-bottom deque) (1+ t-val)) nil))))
        (t
         ;; Empty
         (setf (wsd-bottom deque) t-val)
         nil)))))

(defun wsd-steal (deque)
  "Thief steals from the top of DEQUE. Returns NIL if empty or contention."
  (let ((t-val (the fixnum (wsd-top deque))))
    (sb-thread:barrier (:read))
    (let ((b (the fixnum (wsd-bottom deque))))
      (if (< t-val b)
          (let ((item (svref (wsd-buffer deque)
                             (logand t-val (1- (length (wsd-buffer deque)))))))
            (if (eq (sb-ext:cas (wsd-top deque) t-val (1+ t-val)) t-val)
                item
                nil))
          nil))))

;;;; ===== Fiber struct =====

(defstruct (fiber (:constructor %make-fiber))
  (name nil :type (or null simple-string))
  (state :created :type (member :created :runnable :running :suspended :dead))
  ;; Control stack (mmap'd region)
  (control-stack-start 0 :type sb-vm:word)   ; mmap base (low addr, includes guard page)
  (control-stack-end   0 :type sb-vm:word)   ; base + size (high addr, stack top)
  (control-stack-size  0 :type fixnum)       ; bytes allocated
  ;; Saved RSP (the only register stored outside the stack itself)
  (saved-rsp 0 :type sb-vm:word)
  ;; Binding stack (separate mmap'd region)
  (binding-stack-start   0 :type sb-vm:word)
  (binding-stack-pointer 0 :type sb-vm:word)
  (binding-stack-size    0 :type fixnum)
  ;; TLS overlay: parallel arrays of tls-index and current-value (raw words).
  (tls-indices nil :type (or null (simple-array sb-vm:word (*))))
  (tls-values  nil :type (or null (simple-array sb-vm:word (*))))
  ;; NLX chain pointers
  (saved-catch-block 0 :type sb-vm:word)
  (saved-unwind-protect-block 0 :type sb-vm:word)
  ;; Entry function and result
  (function nil :type (or null function))
  (result nil)
  ;; Scheduling
  (carrier nil)    ; sb-thread:thread this fiber runs on
  (scheduler nil)  ; back-pointer to fiber-scheduler
  ;; Wake condition (predicate for scheduler to check)
  (wake-condition nil :type (or null function))
  ;; I/O wait info (inlined — fd + direction for idle hook batching)
  (wait-fd -1 :type fixnum)
  (wait-direction :input :type (member :input :output))
  ;; True when this fiber is parked in scheduler io-waiters table.
  (io-indexed-p nil :type t)
  ;; Deadline for timed waits (internal-real-time units), or NIL
  (deadline nil :type (or null fixnum))
  ;; Index in scheduler's deadline min-heap (-1 = not in heap)
  (heap-index -1 :type fixnum)
  ;; Intrusive linked list pointer for waiting list (avoids cons allocation)
  (next-waiting nil :type (or null fiber))
  ;; Pinning (prevents unmounting)
  (pin-count 0 :type fixnum)
  ;; GC info pointer (SAP to struct fiber_gc_info)
  (gc-info (int-sap 0) :type system-area-pointer)
  ;; C stack descriptor pointers (for freeing)
  (c-control-stack (int-sap 0) :type system-area-pointer)
  (c-binding-stack (int-sap 0) :type system-area-pointer))

;;;; ===== Fiber Scheduler Group (multi-carrier work-stealing) =====

(defstruct (fiber-scheduler-group (:constructor %make-fiber-scheduler-group)
                                   (:conc-name fsg-))
  ;; Vector of all schedulers in this group
  (schedulers #() :type simple-vector)
  ;; Total active (non-dead) fibers across all carriers.
  ;; Typed T (holds fixnum) so CAS works for atomic decrement.
  (active-count 0 :type t)
  ;; Carrier threads (all background, for start-fibers/finish-fibers)
  (threads nil :type list)
  ;; Fibers submitted to this group (for result collection).
  ;; Typed T so that CAS/ATOMIC-PUSH works for dynamic submission.
  (fibers nil :type t))

;;;; ===== Fiber Scheduler struct =====

(defstruct (fiber-scheduler (:constructor %make-fiber-scheduler))
  (carrier nil)                                       ; slot 0
  (run-deque nil :type t)                             ; slot 1 (was run-queue)
  (waiting nil :type (or null fiber))                  ; slot 2 (intrusive list head)
  (current-fiber nil :type (or null fiber))           ; slot 3
  ;; Saved RSP for the scheduler's own stack frame (carrier thread stack)
  (saved-rsp 0 :type sb-vm:word)                     ; slot 4 — MUST stay here
  ;; Carrier's binding-stack-pointer, saved when a fiber is mounted
  (carrier-bsp 0 :type sb-vm:word)
  ;; Carrier's catch/unwind-protect block pointers
  (carrier-catch-block 0 :type sb-vm:word)
  (carrier-unwind-protect-block 0 :type sb-vm:word)
  ;; Hook called when no fibers are runnable but some are waiting
  (idle-hook nil :type (or null function))
  ;; Event multiplexer fd: epoll (Linux), kqueue (BSD), -1 = unavailable
  (event-fd -1 :type fixnum)
  ;; Hash table of fd -> registered-events-mask (for epoll ctl tracking)
  (registered-fds nil :type (or null hash-table))
  ;; Linux fast path: fd -> list of parked fibers waiting on epoll readiness.
  ;; Only used for waiters without explicit deadlines.
  (io-waiters (make-hash-table) :type hash-table)
  (io-waiter-count 0 :type fixnum)
  ;; Work-stealing group (multi-carrier only)
  (group nil :type (or null fiber-scheduler-group))
  ;; Index of this scheduler in the group's schedulers vector
  (index 0 :type fixnum)
  ;; Thread-safe pending queue for external fiber submissions.
  ;; Non-owner threads (e.g., the hunchentoot accept thread) push here
  ;; via sb-ext:atomic-push instead of directly pushing to the run-deque
  ;; (which is only safe for the owner carrier thread).
  (pending-fibers nil :type t)
  ;; Pre-allocated buffers for epoll-driven I/O wake (Linux only)
  (ready-fds (make-hash-table) :type hash-table)
  (epoll-buf (make-array 64 :element-type '(signed-byte 32))
             :type (simple-array (signed-byte 32) (*)))
  ;; Per-scheduler deadline min-heap (binary heap ordered by fiber-deadline)
  (deadline-heap (make-array 64) :type simple-vector)
  (deadline-heap-count 0 :type fixnum)
  ;; Scratch hash table for TLS save/restore (avoids consing per context switch)
  (tls-scratch (make-hash-table :size 32) :type hash-table))

;;;; ===== Portable TLS offset helpers =====
;;;
;;; On architectures with wired TLS slots (x86-64, arm64, riscv, loongarch64),
;;; catch-block and unwind-protect-block have compile-time slot constants.
;;; On others (PPC64), these are regular TLS variables accessed via
;;; symbol-tls-index at runtime.

(defun %catch-block-tls-offset ()
  "Return the TLS byte offset for *current-catch-block*."
  #+(or x86-64 (and (or arm64 riscv loongarch64) sb-thread))
  (ash sb-vm::thread-current-catch-block-slot sb-vm:word-shift)
  #-(or x86-64 (and (or arm64 riscv loongarch64) sb-thread))
  (symbol-tls-index 'sb-vm::*current-catch-block*))

(defun %uwp-block-tls-offset ()
  "Return the TLS byte offset for *current-unwind-protect-block*."
  #+(or x86-64 (and (or arm64 riscv loongarch64) sb-thread))
  (ash sb-vm::thread-current-unwind-protect-block-slot sb-vm:word-shift)
  #-(or x86-64 (and (or arm64 riscv loongarch64) sb-thread))
  (symbol-tls-index 'sb-vm::*current-unwind-protect-block*))

;;;; ===== Global Fiber Registry =====

(defvar *all-fibers* nil)
(defvar *all-fibers-lock* (sb-thread:make-mutex :name "all-fibers"))

(defun list-all-fibers ()
  "Return a list of all live fibers."
  (sb-thread:with-mutex (*all-fibers-lock*)
    (copy-list *all-fibers*)))

;;;; ===== Fiber printing =====

(defmethod print-object ((fiber fiber) stream)
  (print-unreadable-object (fiber stream :type t :identity t)
    (let ((name (fiber-name fiber))
          (state (fiber-state fiber))
          (carrier (fiber-carrier fiber)))
      (when name (format stream "~S " name))
      (format stream "~S" state)
      (when (and carrier (eq state :running))
        (format stream " on ~S" carrier)))))

(defmethod print-object ((group fiber-scheduler-group) stream)
  (print-unreadable-object (group stream :type t :identity t)
    (format stream "~D carrier~:P, ~D active"
            (length (fsg-schedulers group))
            (fsg-active-count group))))

;;;; ===== Per-thread scheduler =====
;;;
;;; *current-fiber* and *current-scheduler* are declared in thread-structs.lisp
;;; (build order #146) so that serve-event.lisp (#522) can reference them.

(declaim (inline current-fiber current-scheduler))
(defun current-fiber () *current-fiber*)
(defun current-scheduler () *current-scheduler*)

;;;; ===== Fiber pinning =====

(declaim (inline fiber-can-yield-p))
(defun fiber-can-yield-p (&optional (fiber (current-fiber)))
  (and fiber (zerop (fiber-pin-count fiber))))

(defun fiber-pin (&optional (fiber (current-fiber)))
  (when fiber
    (incf (fiber-pin-count fiber))))

(defun fiber-unpin (&optional (fiber (current-fiber)))
  (when fiber
    (let ((count (fiber-pin-count fiber)))
      (when (plusp count)
        (setf (fiber-pin-count fiber) (1- count))))))

(defmacro with-fiber-pinned ((&optional (fiber '(current-fiber))) &body body)
  (let ((f (gensym "FIBER")))
    `(let ((,f ,fiber))
       (when ,f (fiber-pin ,f))
       (unwind-protect
            (progn ,@body)
         (when ,f (fiber-unpin ,f))))))

;;;; ===== Address helpers for fiber_switch =====
;;;
;;; fiber_switch needs pointers to the saved-rsp slots.
;;; These return raw addresses (unsigned integers) of the saved-rsp slot.
;;; They do NOT allocate SAP objects, which is critical because SAP allocation
;;; can trigger GC and move the struct, invalidating the address.

(defun fiber-saved-rsp-addr (fiber)
  "Return the raw address of the saved-rsp slot of FIBER.
Returns an unsigned integer (no SAP allocation)."
  (+ (sb-kernel:get-lisp-obj-address fiber)
     (- sb-vm:instance-pointer-lowtag)
     (* (+ 1 ; header word
           sb-vm:instance-data-start
           ;; slot index of saved-rsp in the fiber struct
           ;; We count: name=0, state=1, control-stack-start=2,
           ;;           control-stack-end=3, control-stack-size=4,
           ;;           saved-rsp=5
           5)
        sb-vm:n-word-bytes)))

(defun scheduler-saved-rsp-addr (scheduler)
  "Return the raw address of the saved-rsp slot of SCHEDULER.
Returns an unsigned integer (no SAP allocation)."
  (+ (sb-kernel:get-lisp-obj-address scheduler)
     (- sb-vm:instance-pointer-lowtag)
     (* (+ 1 ; header word
           sb-vm:instance-data-start
           ;; slot index: carrier=0, run-deque=1, waiting=2,
           ;;             current-fiber=3, saved-rsp=4
           4)
        sb-vm:n-word-bytes)))

;;;; ===== Stack initialization =====

(defun fiber-entry-trampoline-address ()
  "Return the address of the fiber-entry-trampoline assembly routine."
  (sb-fasl:get-asm-routine 'sb-vm::fiber-entry-trampoline))

(defun initialize-fiber-stack (fiber)
  "Set up the initial stack frame for a brand-new fiber so that
fiber_switch 'returns' into fiber_entry_trampoline."
  (let* ((stack-start (fiber-control-stack-start fiber))
         (stack-size (fiber-control-stack-size fiber))
         (stack-top (+ stack-start stack-size))
         (trampoline-addr (fiber-entry-trampoline-address))
         (fiber-lispobj (sb-kernel:get-lisp-obj-address fiber)))
    (let ((sap (sb-sys:int-sap stack-top)))

      #+arm64
      ;; ARM64 (AAPCS64): 20 callee-saved regs, 160 bytes.
      ;; fiber_switch saves: x29, x30, x19-x28, d8-d15.
      ;; Initial frame has fiber_lispobj in x29 slot, trampoline in x30 slot.
      ;;
      ;; Stack layout (offsets from stack_top, growing downward):
      ;;   -0x08: [alignment padding: 0]   (16-byte stack alignment)
      ;;   -0xA8: x29 = fiber_lispobj      (frame base)
      ;;   -0xA0: x30 = trampoline_addr
      ;;   -0x98..-0x60: x19-x28 = 0
      ;;   -0x58..-0x10: d8-d15 = 0
      ;;                 ^--- initial SP = stack_top - 0xA8 (168 bytes)
      (progn
        (setf (sb-sys:sap-ref-word sap -8) 0)    ; alignment padding
        ;; 160-byte frame starts at stack_top - 8 - 160 = stack_top - 168
        ;; But fiber_switch uses "stp x29,x30,[sp,#-160]!" so SP will be
        ;; at frame_base.  We lay the frame at stack_top - 168:
        ;;   frame_base = stack_top - 168
        ;;   frame_base + 0   = x29  (fiber_lispobj)
        ;;   frame_base + 8   = x30  (trampoline)
        ;;   frame_base + 16  = x19  (0)
        ;;   ...
        ;;   frame_base + 144 = d14  (0)
        ;;   frame_base + 152 = d15  (0)
        ;; After frame: stack_top - 8 (alignment padding)
        ;; Then the "ret" on restore will use x30 = trampoline.
        (let ((base (- stack-top 168)))  ; 8 alignment + 160 frame
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)   0) fiber-lispobj)   ; x29
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)   8) trampoline-addr) ; x30
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  16) 0)  ; x19
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  24) 0)  ; x20
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  32) 0)  ; x21
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  40) 0)  ; x22
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  48) 0)  ; x23
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  56) 0)  ; x24
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  64) 0)  ; x25
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  72) 0)  ; x26
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  80) 0)  ; x27
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  88) 0)  ; x28
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base)  96) 0)  ; d8
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 104) 0)  ; d9
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 112) 0)  ; d10
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 120) 0)  ; d11
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 128) 0)  ; d12
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 136) 0)  ; d13
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 144) 0)  ; d14
          (setf (sb-sys:sap-ref-word (sb-sys:int-sap base) 152) 0)  ; d15
          (setf (fiber-saved-rsp fiber) base)))

      #+arm
      ;; ARM32 (AAPCS): 104-byte frame.
      ;; fiber_switch saves: d8-d15 (64 bytes), then r3-r11,lr (40 bytes).
      ;; On restore: vldmia d8-d15, ldmfd r3-r11,lr; bx lr.
      ;; fiber_lispobj goes in r11 slot, trampoline in lr slot.
      ;;
      ;; Stack layout (offsets from frame base, sp points here):
      ;;   +0x00..+0x3F: d8-d15 = 0 (8 doubles, 64 bytes)
      ;;   +0x40: r3  = 0 (padding)
      ;;   +0x44: r4  = 0
      ;;   +0x48: r5  = 0
      ;;   +0x4C: r6  = 0
      ;;   +0x50: r7  = 0
      ;;   +0x54: r8  = 0
      ;;   +0x58: r9  = 0
      ;;   +0x5C: r10 = 0
      ;;   +0x60: r11 = fiber_lispobj
      ;;   +0x64: lr  = trampoline_addr
      ;;                ^--- frame top = base + 104
      ;; Initial SP = stack_top - 104
      (let ((base (- stack-top 104)))
        ;; d8-d15: 8 doubles at offsets 0..56 (each 8 bytes)
        (dotimes (i 8)
          (setf (sb-sys:sap-ref-double (sb-sys:int-sap base) (* i 8)) 0.0d0))
        ;; r3 (padding) through r10: offsets 64..92 (each 4 bytes)
        (dotimes (i 8)  ; r3..r10
          (setf (sb-sys:sap-ref-32 (sb-sys:int-sap base) (+ 64 (* i 4))) 0))
        ;; r11 = fiber_lispobj at offset 96
        (setf (sb-sys:sap-ref-32 (sb-sys:int-sap base) 96) fiber-lispobj)
        ;; lr = trampoline at offset 100
        (setf (sb-sys:sap-ref-32 (sb-sys:int-sap base) 100) trampoline-addr)
        (setf (fiber-saved-rsp fiber) base))

      #+(and x86-64 (not arm64) (not arm))
      ;; x86-64 — original code for SysV and Win64
      (progn
        ;; NOTE: stack_top is page-aligned (mmap), so 16-byte alignment holds.
        ;; Layout (offsets from stack_top, growing downward):
        ;; SysV ABI (6 callee-saved: rbp, rbx, r12-r15):
        ;;   stack_top - 0x08: [alignment padding: 0]
        ;;   stack_top - 0x10: [spare slot: 0]
        ;;   stack_top - 0x18: [fiber_entry_trampoline address] (return addr)
        ;;   stack_top - 0x20: [fiber lispobj] -> popped into rbp
        ;;   stack_top - 0x28: [0] -> popped into rbx
        ;;   stack_top - 0x30: [0] -> popped into r12
        ;;   stack_top - 0x38: [0] -> popped into r13
        ;;   stack_top - 0x40: [0] -> popped into r14
        ;;   stack_top - 0x48: [0] -> popped into r15
        ;;                     ^--- initial RSP (72 bytes from top)
        ;;
        ;; Win64 ABI (8 callee-saved: rbp, rbx, rdi, rsi, r12-r15):
        ;;   stack_top - 0x08: [alignment padding: 0]
        ;;   stack_top - 0x10: [spare slot: 0]
        ;;   stack_top - 0x18: [fiber_entry_trampoline address] (return addr)
        ;;   stack_top - 0x20: [fiber lispobj] -> popped into rbp
        ;;   stack_top - 0x28: [0] -> popped into rbx
        ;;   stack_top - 0x30: [0] -> popped into rdi
        ;;   stack_top - 0x38: [0] -> popped into rsi
        ;;   stack_top - 0x40: [0] -> popped into r12
        ;;   stack_top - 0x48: [0] -> popped into r13
        ;;   stack_top - 0x50: [0] -> popped into r14
        ;;   stack_top - 0x58: [0] -> popped into r15
        ;;   stack_top - 0xF8: [XMM6..XMM15 spill area, 160 bytes]
        ;;                     ^--- initial RSP (248 bytes from top)
        (setf (sb-sys:sap-ref-word sap -8) 0)   ; alignment padding
        (setf (sb-sys:sap-ref-word sap -16) 0)  ; spare slot
        (setf (sb-sys:sap-ref-word sap -24) trampoline-addr)  ; return addr
        (setf (sb-sys:sap-ref-word sap -32) fiber-lispobj)    ; -> rbp
        (setf (sb-sys:sap-ref-word sap -40) 0)  ; -> rbx
        #+win32
        (progn
          (setf (sb-sys:sap-ref-word sap -48) 0)  ; -> rdi
          (setf (sb-sys:sap-ref-word sap -56) 0)  ; -> rsi
          (setf (sb-sys:sap-ref-word sap -64) 0)  ; -> r12
          (setf (sb-sys:sap-ref-word sap -72) 0)  ; -> r13
          (setf (sb-sys:sap-ref-word sap -80) 0)  ; -> r14
          (setf (sb-sys:sap-ref-word sap -88) 0)  ; -> r15
          ;; XMM6..XMM15 spill area (10 registers * 16 bytes)
          (let ((xmm-base (- stack-top 248)))
            (dotimes (i 20)
              (setf (sb-sys:sap-ref-word (sb-sys:int-sap xmm-base)
                                         (* i sb-vm:n-word-bytes))
                    0)))
          ;; Initial RSP = stack_top - 248
          (setf (fiber-saved-rsp fiber) (- stack-top 248)))
        #-win32
        (progn
          (setf (sb-sys:sap-ref-word sap -48) 0)  ; -> r12
          (setf (sb-sys:sap-ref-word sap -56) 0)  ; -> r13
          (setf (sb-sys:sap-ref-word sap -64) 0)  ; -> r14
          (setf (sb-sys:sap-ref-word sap -72) 0)  ; -> r15
          ;; Initial RSP = stack_top - 72 (6 regs * 8 + return addr + padding)
          (setf (fiber-saved-rsp fiber) (- stack-top 72))))

      #+ppc64
      ;; PPC64 (ELFv2): 320-byte frame.
      ;; fiber-switch saves: LR, CR, r14-r31 (18 GPRs), f14-f31 (18 FPRs).
      ;; Initial frame has trampoline in LR slot, fiber_lispobj in r15 (cfp) slot.
      ;;
      ;; Frame layout (offsets from frame base = saved SP):
      ;;   +0:   backchain (0)
      ;;   +8:   LR = trampoline_addr
      ;;   +16:  CR = 0
      ;;   +24:  r14 = 0 (bsp)
      ;;   +32:  r15 = fiber_lispobj (cfp, used by trampoline)
      ;;   +40..+160: r16-r31 = 0
      ;;   +168..+304: f14-f31 = 0
      ;;   +312..+319: padding
      ;;   saved SP = stack_top - 320
      (let ((base (- stack-top 320)))
        (let ((base-sap (sb-sys:int-sap base)))
          ;; Backchain, LR, CR
          (setf (sb-sys:sap-ref-word base-sap 0) 0)               ; backchain
          (setf (sb-sys:sap-ref-word base-sap 8) trampoline-addr) ; LR
          (setf (sb-sys:sap-ref-word base-sap 16) 0)              ; CR
          ;; GPRs r14-r31 (18 regs at offsets 24..160)
          (loop for offset from 24 to 160 by 8
                do (setf (sb-sys:sap-ref-word base-sap offset) 0))
          ;; r15 (cfp) = fiber lispobj, at offset 32
          (setf (sb-sys:sap-ref-word base-sap 32) fiber-lispobj)
          ;; FPRs f14-f31 (18 regs at offsets 168..304)
          (loop for offset from 168 to 304 by 8
                do (setf (sb-sys:sap-ref-word base-sap offset) 0))
          (setf (fiber-saved-rsp fiber) base))))))

;;;; ===== Fiber creation =====

(defun make-fiber (function &key (name nil)
                                  (stack-size +default-fiber-stack-size+)
                                  (binding-stack-size +default-fiber-binding-stack-size+)
                                  initial-bindings)
  "Create a new fiber that will execute FUNCTION when started.
FUNCTION should be a function of no arguments.
INITIAL-BINDINGS is an alist of (SYMBOL . VALUE-FORM) pairs that are
established as dynamic bindings before FUNCTION runs, similar to
BORDEAUX-THREADS:MAKE-THREAD.  The value forms are evaluated at
MAKE-FIBER time."
  ;; Wrap function with initial bindings if specified
  (when initial-bindings
    (let ((bindings (loop for (sym . val) in initial-bindings
                         collect (cons sym val))))
      (let ((inner function))
        (setf function
              (lambda ()
                (progv (mapcar #'car bindings)
                       (mapcar #'cdr bindings)
                  (funcall inner)))))))
  (let ((cstack-sap (alloc-fiber-stack stack-size)))
    (when (sb-sys:sap= cstack-sap (sb-sys:int-sap 0))
      (error "Failed to allocate fiber control stack"))
    (let ((bstack-sap (alloc-fiber-binding-stack binding-stack-size)))
      (when (sb-sys:sap= bstack-sap (sb-sys:int-sap 0))
        (free-fiber-stack cstack-sap)
        (error "Failed to allocate fiber binding stack"))
      ;; Extract the base and size from the C struct fiber_stack
      ;; struct fiber_stack { void* base; size_t size; size_t guard_size; }
      (let* ((cstack-base (sb-sys:sap-ref-word cstack-sap 0))
             (cstack-total-size (sb-sys:sap-ref-word cstack-sap sb-vm:n-word-bytes))
             (cstack-guard-size (sb-sys:sap-ref-word cstack-sap (* 2 sb-vm:n-word-bytes)))
             ;; Usable stack starts above the guard page
             (cstack-usable-start (+ cstack-base cstack-guard-size))
             (cstack-usable-size (- cstack-total-size cstack-guard-size))
             (bstack-base (sb-sys:sap-ref-word bstack-sap 0))
             (bstack-total-size (sb-sys:sap-ref-word bstack-sap sb-vm:n-word-bytes))
             ;; Create GC info
             (gc-info-sap (make-fiber-gc-info))
             (fiber (%make-fiber
                     :name name
                     :function function
                     :control-stack-start cstack-usable-start
                     :control-stack-end (+ cstack-usable-start cstack-usable-size)
                     :control-stack-size cstack-usable-size
                     :binding-stack-start bstack-base
                     :binding-stack-pointer bstack-base  ; empty initially
                     :binding-stack-size bstack-total-size
                     :gc-info gc-info-sap
                     :c-control-stack cstack-sap
                     :c-binding-stack bstack-sap)))
        (let ((completed nil))
          (unwind-protect
               (progn
                 ;; Initialize the stack frame for fiber_switch
                 (initialize-fiber-stack fiber)
                 ;; Register for GC scanning immediately.  The initial stack frame
                 ;; contains the fiber lispobj (a raw tagged pointer).  If GC runs
                 ;; before this fiber starts, the conservative scanner must see that
                 ;; reference to pin the fiber object in place.
                 (register-fiber-for-gc-from-fiber fiber)
                 ;; Register in global fiber list
                 (with-mutex (*all-fibers-lock*)
                   (push fiber *all-fibers*))
                 (setf completed t)
                 fiber)
            (unless completed
              (free-fiber-gc-info gc-info-sap)
              (free-fiber-stack bstack-sap)
              (free-fiber-stack cstack-sap))))))))

(defun destroy-fiber (fiber)
  "Free the resources associated with FIBER. Only call on dead fibers."
  (unless (eq (fiber-state fiber) :dead)
    (error "Cannot destroy a fiber that is not dead"))
  ;; Remove from global fiber list
  (with-mutex (*all-fibers-lock*)
    (setf *all-fibers* (delete fiber *all-fibers* :count 1)))
  ;; Free GC info
  (let ((gc-info (fiber-gc-info fiber)))
    (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
      (free-fiber-gc-info gc-info)
      (setf (fiber-gc-info fiber) (sb-sys:int-sap 0))))
  ;; Free stacks
  (let ((cstack (fiber-c-control-stack fiber)))
    (unless (sb-sys:sap= cstack (sb-sys:int-sap 0))
      (free-fiber-stack cstack)
      (setf (fiber-c-control-stack fiber) (sb-sys:int-sap 0))))
  (let ((bstack (fiber-c-binding-stack fiber)))
    (unless (sb-sys:sap= bstack (sb-sys:int-sap 0))
      (free-fiber-stack bstack)
      (setf (fiber-c-binding-stack fiber) (sb-sys:int-sap 0)))))

;;;; ===== TLS / Binding Stack Management =====
;;;
;;; SBCL's unbind_to_here() ZEROES binding stack entries after restoring
;;; TLS values.  This means we CANNOT use it during fiber yield -- the
;;; fiber's binding stack entries must remain intact for normal Lisp
;;; unbinding (let block exits, handler-case cleanup, etc.) when the
;;; fiber is resumed.
;;;
;;; Instead, we directly swap TLS values:
;;;   On yield:  save fiber's current TLS -> overlay, restore carrier's
;;;              values from the outermost binding stack entry's old_value
;;;   On resume: write fiber's saved values from overlay back to TLS
;;;
;;; The binding stack entries are never modified by the fiber scheduler.

(defun save-fiber-tls-and-restore-carrier (fiber scheduler)
  "Save the fiber's current TLS values and restore the carrier's TLS values.
Walk the binding stack from bs_start to BSP (oldest to newest).  For each
unique TLS index, the FIRST (outermost) entry's old_value is the carrier's
original TLS value.  Save the fiber's current TLS value, then write the
carrier's value back to TLS.  Does NOT modify binding stack entries.
Uses the scheduler's tls-scratch hash table to avoid consing for dedup.
Reuses existing overlay arrays when the unique binding count is unchanged."
  (let* ((bsp (sb-sys:int-sap (fiber-binding-stack-pointer fiber)))
         (bs-start (sb-sys:int-sap (fiber-binding-stack-start fiber)))
         (thread-sap (current-thread-sap))
         (scratch (fiber-scheduler-tls-scratch scheduler)))
    ;; Pass 1: Count unique TLS indices using scratch hash table
    (clrhash scratch)
    (let ((count 0)
          (ptr bs-start))
      (declare (fixnum count))
      (loop while (sb-sys:sap< ptr bsp)
            do (let ((tls-index (sb-sys:sap-ref-word ptr sb-vm:n-word-bytes)))
                 (when (and (plusp tls-index)
                            (not (gethash tls-index scratch)))
                   (setf (gethash tls-index scratch) t)
                   (incf count)))
               (setf ptr (sb-sys:sap+ ptr (* 2 sb-vm:n-word-bytes))))
      (if (zerop count)
          (setf (fiber-tls-indices fiber) nil
                (fiber-tls-values fiber) nil)
          ;; Reuse existing arrays if count matches, otherwise allocate new ones
          (let ((indices (let ((old (fiber-tls-indices fiber)))
                           (if (and old (= (length old) count))
                               old
                               (make-array count :element-type 'sb-vm:word))))
                (values (let ((old (fiber-tls-values fiber)))
                          (if (and old (= (length old) count))
                              old
                              (make-array count :element-type 'sb-vm:word)))))
            ;; Pass 2: Walk again, fill arrays, swap TLS values
            ;; Clear scratch for dedup in this pass
            (clrhash scratch)
            (let ((i 0)
                  (ptr2 bs-start))
              (declare (fixnum i))
              (loop while (sb-sys:sap< ptr2 bsp)
                    do (let ((old-value (sb-sys:sap-ref-word ptr2 0))
                             (tls-index (sb-sys:sap-ref-word ptr2 sb-vm:n-word-bytes)))
                         (when (and (plusp tls-index)
                                    (not (gethash tls-index scratch)))
                           (setf (gethash tls-index scratch) t)
                           ;; Save fiber's current TLS value
                           (setf (aref indices i) tls-index)
                           (setf (aref values i)
                                 (sb-sys:sap-ref-word thread-sap tls-index))
                           ;; Restore carrier's TLS value (outermost old_value)
                           (setf (sb-sys:sap-ref-word thread-sap tls-index)
                                 old-value)
                           (incf i)))
                       (setf ptr2 (sb-sys:sap+ ptr2 (* 2 sb-vm:n-word-bytes)))))
            (setf (fiber-tls-indices fiber) indices
                  (fiber-tls-values fiber) values))))))

(defun restore-fiber-tls-overlay (fiber)
  "Restore the fiber's TLS values from its saved overlay.
TLS index is a byte offset into the thread struct.
Values are raw words stored directly."
  (let ((indices (fiber-tls-indices fiber))
        (values (fiber-tls-values fiber))
        (thread-sap (current-thread-sap)))
    (when (and indices values)
      (dotimes (i (length indices))
        (setf (sb-sys:sap-ref-word thread-sap (aref indices i))
              (aref values i))))))

(defun update-binding-stack-carrier-values (fiber thread-sap scheduler)
  "Update the binding stack's outermost old_values to reflect the current
carrier's TLS values.  When a fiber migrates between carriers via work-stealing,
the binding stack still contains the old carrier's TLS values as old_values.
This must be called before restoring the fiber's TLS overlay so that both
the yield path and the death path see correct carrier values.
Uses the scheduler's tls-scratch hash table to avoid consing."
  (let* ((bsp (sb-sys:int-sap (fiber-binding-stack-pointer fiber)))
         (bs-start (sb-sys:int-sap (fiber-binding-stack-start fiber)))
         (scratch (fiber-scheduler-tls-scratch scheduler)))
    (clrhash scratch)
    (let ((ptr bs-start))
      (loop while (sb-sys:sap< ptr bsp)
            do (let ((tls-index (sb-sys:sap-ref-word ptr sb-vm:n-word-bytes)))
                 (when (and (plusp tls-index)
                            (not (gethash tls-index scratch)))
                   (setf (gethash tls-index scratch) t)
                   ;; Overwrite old_value with current carrier's TLS value
                   (setf (sb-sys:sap-ref-word ptr 0)
                         (sb-sys:sap-ref-word thread-sap tls-index))))
               (setf ptr (sb-sys:sap+ ptr (* 2 sb-vm:n-word-bytes)))))))

;;;; ===== Fiber Yield =====

(defun fiber-yield (&optional wake-condition)
  "Yield the current fiber. If WAKE-CONDITION is provided, it should
be a function of no arguments that returns true when the fiber is
ready to be resumed."
  (let ((fiber (current-fiber))
        (sched (current-scheduler)))
    (unless (and fiber sched)
      (error "fiber-yield called outside of a fiber context"))
    (unless (fiber-can-yield-p fiber)
      (error "Cannot yield: fiber is pinned (pin-count=~D)"
             (fiber-pin-count fiber)))
    (sb-sys:without-interrupts
      (let* ((thread-sap (current-thread-sap))
             (bsp-offset (ash sb-vm::thread-binding-stack-pointer-slot
                              sb-vm:word-shift))
             (catch-offset (%catch-block-tls-offset))
             (uwp-offset (%uwp-block-tls-offset)))
        ;; 1. Save current BSP as fiber's binding-stack-pointer
        (setf (fiber-binding-stack-pointer fiber)
              (sb-sys:sap-ref-word thread-sap bsp-offset))
        ;; 1b. Save fiber's catch/UWP blocks
        (setf (fiber-saved-catch-block fiber)
              (sb-sys:sap-ref-word thread-sap catch-offset))
        (setf (fiber-saved-unwind-protect-block fiber)
              (sb-sys:sap-ref-word thread-sap uwp-offset))
        ;; 2. Save fiber's TLS values and restore carrier's TLS values.
        ;;    This does NOT call unbind_to_here (which would zero entries).
        ;;    Instead it directly swaps TLS slot values using the binding
        ;;    stack entries' old_values as the carrier's original state.
        (save-fiber-tls-and-restore-carrier fiber sched)
        ;; 3. Restore carrier's BSP and catch/UWP blocks
        (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
              (fiber-scheduler-carrier-bsp sched))
        (setf (sb-sys:sap-ref-word thread-sap catch-offset)
              (fiber-scheduler-carrier-catch-block sched))
        (setf (sb-sys:sap-ref-word thread-sap uwp-offset)
              (fiber-scheduler-carrier-unwind-protect-block sched))
        ;; 4. Set fiber state and wake condition
        (setf (fiber-state fiber) :suspended
              (fiber-wake-condition fiber) wake-condition)
        ;; 5. Context switch to scheduler.
        ;;    NOTE: GC registration happens in resume-fiber-internal step 8,
        ;;    AFTER fiber_switch returns, when saved-rsp has the correct value.
        ;;    We must NOT register here because fiber_switch hasn't saved RSP yet.
        (%fiber-switch (fiber-saved-rsp-addr fiber)
                      (scheduler-saved-rsp-addr sched)
                      0)
        ;; Returns here when fiber is resumed
        ))))

(defun register-fiber-for-gc-from-fiber (fiber)
  "Update the fiber's gc-info and register it for GC scanning."
  (let ((gc-info (fiber-gc-info fiber)))
    (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
      ;; Update the gc_info fields
      ;; struct fiber_gc_info {
      ;;   lispobj* control_stack_base;     // offset 0
      ;;   lispobj* control_stack_pointer;  // offset 8
      ;;   lispobj* control_stack_end;      // offset 16
      ;;   lispobj* binding_stack_start;    // offset 24
      ;;   lispobj* binding_stack_pointer;  // offset 32
      ;;   ... next/prev pointers           // offset 40, 48
      ;; }
      (setf (sb-sys:sap-ref-word gc-info 0) (fiber-control-stack-start fiber))
      (setf (sb-sys:sap-ref-word gc-info sb-vm:n-word-bytes) (fiber-saved-rsp fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 2 sb-vm:n-word-bytes)) (fiber-control-stack-end fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 3 sb-vm:n-word-bytes)) (fiber-binding-stack-start fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 4 sb-vm:n-word-bytes)) (fiber-binding-stack-pointer fiber))
      (sb-sys:without-gcing
        (register-fiber-for-gc gc-info)))))

(defun unregister-fiber-gc-roots (fiber)
  "Unregister fiber from GC scanning (called before resuming it)."
  (let ((gc-info (fiber-gc-info fiber)))
    (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
      (sb-sys:without-gcing
        (unregister-fiber-for-gc gc-info)))))

;;;; ===== Resume Fiber =====

(defun start-fiber (scheduler fiber)
  "Start a newly created fiber for the first time."
  (setf (fiber-carrier fiber) *current-thread*
        (fiber-scheduler fiber) scheduler
        (fiber-state fiber) :running)
  (resume-fiber-internal scheduler fiber))

(defun resume-fiber (scheduler fiber)
  "Resume a suspended fiber."
  (setf (fiber-state fiber) :running)
  (resume-fiber-internal scheduler fiber))

(defun resume-fiber-internal (scheduler fiber)
  "Common resume path for both new and suspended fibers."
  (sb-sys:without-interrupts
    (let* ((thread-sap (current-thread-sap))
           (bsp-offset (ash sb-vm::thread-binding-stack-pointer-slot
                            sb-vm:word-shift))
           (catch-offset (%catch-block-tls-offset))
           (uwp-offset (%uwp-block-tls-offset))
           (carrier-bsp (sb-sys:sap-ref-word thread-sap bsp-offset))
           (carrier-stack-start
             (sb-sys:sap-ref-word thread-sap
               (ash sb-vm::thread-control-stack-start-slot sb-vm:word-shift)))
           (carrier-stack-end
             (sb-sys:sap-ref-word thread-sap
               (ash sb-vm::thread-control-stack-end-slot sb-vm:word-shift)))
           (carrier-bs-start
             (sb-sys:sap-ref-word thread-sap
               (ash sb-vm::thread-binding-stack-start-slot sb-vm:word-shift))))
      ;; 1. Save carrier's BSP, catch block, and UWP block
      (setf (fiber-scheduler-carrier-bsp scheduler) carrier-bsp)
      (setf (fiber-scheduler-carrier-catch-block scheduler)
            (sb-sys:sap-ref-word thread-sap catch-offset))
      (setf (fiber-scheduler-carrier-unwind-protect-block scheduler)
            (sb-sys:sap-ref-word thread-sap uwp-offset))
      ;; 2. Register GC context FIRST: tell GC about the fiber's stack
      ;;    boundaries and register the carrier's suspended stack for
      ;;    scanning.  This must happen BEFORE changing BSP so that if
      ;;    stop-the-world GC fires between here and step 4, the GC
      ;;    can find the fiber context for this thread.
      (enter-fiber-gc-context
        (fiber-control-stack-start fiber)
        (fiber-control-stack-end fiber)
        (fiber-binding-stack-start fiber)
        carrier-stack-start carrier-stack-end
        carrier-bs-start carrier-bsp)
      ;; 3. Set thread BSP to fiber's binding-stack-pointer
      (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
            (fiber-binding-stack-pointer fiber))
      ;; 3b. Restore fiber's catch/UWP blocks
      ;;     (0 for new fibers, saved values for suspended fibers)
      (setf (sb-sys:sap-ref-word thread-sap catch-offset)
            (fiber-saved-catch-block fiber))
      (setf (sb-sys:sap-ref-word thread-sap uwp-offset)
            (fiber-saved-unwind-protect-block fiber))
      ;; 3c. Update binding stack carrier values for migration.
      ;;     When a fiber migrates between carriers, the binding stack's
      ;;     outermost old_values still reflect the old carrier's TLS.
      ;;     Patch them to the current carrier's values before restoring
      ;;     the TLS overlay, so yield/death paths see correct state.
      (update-binding-stack-carrier-values fiber thread-sap scheduler)
      ;; 4. Replay fiber's TLS overlay
      (restore-fiber-tls-overlay fiber)
      ;; 5. Widen fiber's GC scan range to cover the entire stack.
      ;;    Do NOT unregister the fiber yet — there is a window between
      ;;    here and fiber_switch (step 8) where the carrier's RSP is
      ;;    still in the carrier stack.  The active_fiber_context only
      ;;    kicks in when RSP is in the fiber's stack, so the fiber's
      ;;    gc_info in all_fiber_gc_info is the only path that keeps
      ;;    the fiber's stack visible to GC during this window.
      ;;    Setting control_stack_pointer to control_stack_start means
      ;;    "scan the whole fiber stack" — conservative but safe.
      (let ((gc-info (fiber-gc-info fiber)))
        (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
          (sb-sys:without-gcing
            (setf (sb-sys:sap-ref-word gc-info sb-vm:n-word-bytes)
                  (fiber-control-stack-start fiber)))))
      ;; 6. Set effective stack bounds to the fiber's stack so that
      ;;    dynamic-extent overflow checks and stack-allocated-p use
      ;;    the fiber's bounds.  Does NOT touch control-stack-start/end
      ;;    (used by GC).  These are per-thread struct fields, so
      ;;    thread-safe across carriers.
      (setf (sb-sys:sap-ref-word thread-sap
              (ash sb-vm::thread-effective-control-stack-start-slot sb-vm:word-shift))
            (fiber-control-stack-start fiber))
      (setf (sb-sys:sap-ref-word thread-sap
              (ash sb-vm::thread-effective-control-stack-end-slot sb-vm:word-shift))
            (fiber-control-stack-end fiber))
      ;; 7. Set current fiber
      (let ((old-fiber *current-fiber*)
            (old-sched *current-scheduler*))
        (setf *current-fiber* fiber
              *current-scheduler* scheduler)
        ;; 8. Context switch to fiber.
        (%fiber-switch (scheduler-saved-rsp-addr scheduler)
                      (fiber-saved-rsp-addr fiber)
                      (sb-sys:sap-int thread-sap))
        ;; Returns here when fiber yields or dies.
        (setf *current-fiber* old-fiber
              *current-scheduler* old-sched))
      ;; 9. Restore effective stack bounds to carrier's bounds.
      (setf (sb-sys:sap-ref-word thread-sap
              (ash sb-vm::thread-effective-control-stack-start-slot sb-vm:word-shift))
            carrier-stack-start)
      (setf (sb-sys:sap-ref-word thread-sap
              (ash sb-vm::thread-effective-control-stack-end-slot sb-vm:word-shift))
            carrier-stack-end)
      ;; 10. Update fiber GC registration.
      ;;     Dead fibers: unregister (gc_info was kept registered through
      ;;     fiber_switch for safety; now safe to remove before destroy-fiber
      ;;     frees the gc_info struct).
      ;;     Live fibers: update gc_info with the new saved RSP/BSP
      ;;     (register-fiber-for-gc-from-fiber is a no-op for the
      ;;     register call since the fiber is already registered, but
      ;;     it updates all the gc_info fields first).
      (if (eq (fiber-state fiber) :dead)
          (unregister-fiber-gc-roots fiber)
          (register-fiber-for-gc-from-fiber fiber))
      ;; 11. Leave fiber GC context (unregister carrier's stack)
      (leave-fiber-gc-context)
      ;; 11. Restore carrier's BSP and catch/UWP blocks
      (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
            (fiber-scheduler-carrier-bsp scheduler))
      (setf (sb-sys:sap-ref-word thread-sap catch-offset)
            (fiber-scheduler-carrier-catch-block scheduler))
      (setf (sb-sys:sap-ref-word thread-sap uwp-offset)
            (fiber-scheduler-carrier-unwind-protect-block scheduler)))))

;;;; ===== Fiber trampoline (called from C) =====

(defun fiber-trampoline (fiber)
  "Entry point for a new fiber. Called from fiber_run_and_finish via
the registered Lisp trampoline. Runs the fiber's function with
error handling, marks it dead, cleans up bindings, and switches
back to the scheduler."
  (unwind-protect
       (handler-case
           (let ((fn (fiber-function fiber)))
             (when fn
               (setf (fiber-result fiber) (funcall fn))))
         (error (c)
           (setf (fiber-result fiber) c)))
    ;; Cleanup: mark dead, restore carrier TLS and BSP, switch back.
    ;; IMPORTANT: Re-read the scheduler from the fiber struct, NOT from
    ;; a captured variable.  The fiber may have migrated between carriers
    ;; (via work-stealing), so the scheduler at creation time may differ
    ;; from the current one.
    ;; Wrapped in without-interrupts to prevent GC stop signals from
    ;; arriving while thread state (TLS, BSP, catch/UWP) is partially
    ;; restored — a GC during this window sees inconsistent state.
    (sb-sys:without-interrupts
      (setf (fiber-state fiber) :dead)
      (let* ((scheduler (fiber-scheduler fiber))
             (thread-sap (current-thread-sap))
             (bsp-offset (ash sb-vm::thread-binding-stack-pointer-slot
                              sb-vm:word-shift))
             (catch-offset (%catch-block-tls-offset))
             (uwp-offset (%uwp-block-tls-offset))
             (current-bsp (sb-sys:sap-ref-word thread-sap bsp-offset))
             (bs-start (fiber-binding-stack-start fiber)))
        ;; If there are remaining binding stack entries, restore carrier
        ;; TLS values from outermost old_values (same approach as yield)
        (when (> current-bsp bs-start)
          (let ((bsp-sap (sb-sys:int-sap current-bsp))
                (start-sap (sb-sys:int-sap bs-start))
                (scratch (fiber-scheduler-tls-scratch scheduler)))
            (clrhash scratch)
            (let ((ptr start-sap))
              (loop while (sb-sys:sap< ptr bsp-sap)
                    do (let ((old-value (sb-sys:sap-ref-word ptr 0))
                             (tls-index (sb-sys:sap-ref-word ptr sb-vm:n-word-bytes)))
                         (when (and (plusp tls-index)
                                    (not (gethash tls-index scratch)))
                           (setf (gethash tls-index scratch) t)
                           (setf (sb-sys:sap-ref-word thread-sap tls-index)
                                 old-value)))
                    (setf ptr (sb-sys:sap+ ptr (* 2 sb-vm:n-word-bytes)))))))
        ;; Restore carrier's BSP and catch/UWP blocks
        (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
              (fiber-scheduler-carrier-bsp scheduler))
        (setf (sb-sys:sap-ref-word thread-sap catch-offset)
              (fiber-scheduler-carrier-catch-block scheduler))
        (setf (sb-sys:sap-ref-word thread-sap uwp-offset)
              (fiber-scheduler-carrier-unwind-protect-block scheduler))
        ;; Switch back to scheduler
        (%fiber-switch (fiber-saved-rsp-addr fiber)
                      (scheduler-saved-rsp-addr scheduler)
                      0)
        ;; Should not reach here
        (error "fiber-trampoline: fiber_switch returned (unreachable)")))))


;;;; ===== Register the Lisp trampoline with C =====

(define-alien-routine "set_fiber_run_trampoline"
    void (fun unsigned-long))

(defun install-fiber-trampoline ()
  "Register the Lisp fiber trampoline function with the C runtime."
  (set-fiber-run-trampoline
   (sb-kernel:get-lisp-obj-address #'fiber-trampoline)))

;;;; ===== Scheduler =====

(defun make-fiber-scheduler (&key idle-hook)
  "Create a new fiber scheduler for the current thread."
  (let ((sched (%make-fiber-scheduler
                :carrier *current-thread*
                :run-deque (%make-wsd)
                :idle-hook idle-hook)))
    ;; Try to create platform-optimal event multiplexer
    #+(or linux bsd)
    (let ((efd #+linux (sb-unix:epoll-create1 0)
               #+bsd   (sb-unix:kqueue)))
      (when efd  ; nil on error
        (setf (fiber-scheduler-event-fd sched) efd
              (fiber-scheduler-registered-fds sched) (make-hash-table))))
    sched))

(defun submit-fiber (target fiber)
  "Submit a fiber for execution.
TARGET can be a FIBER-SCHEDULER (pushes directly to its deque) or a
FIBER-SCHEDULER-GROUP (atomically increments active-count, picks a random
scheduler, and pushes to its deque)."
  (etypecase target
    (fiber-scheduler
     ;; Direct deque push — only safe from the owner (carrier) thread,
     ;; or during setup before any carrier has started.
     (let ((carrier (fiber-scheduler-carrier target)))
       (when (and carrier (not (eq carrier *current-thread*)))
         (error "submit-fiber to ~S from non-owner thread (owner: ~S, caller: ~S)"
                target carrier *current-thread*)))
     (setf (fiber-scheduler fiber) target
           (fiber-state fiber) :runnable)
     (wsd-push (fiber-scheduler-run-deque target) fiber))
    (fiber-scheduler-group
     ;; Atomically increment active-count so carriers don't exit prematurely
     (loop for old = (fsg-active-count target)
           until (eq (sb-ext:cas (fsg-active-count target) old (1+ old))
                     old))
     ;; Track fiber for result collection by finish-fibers
     (sb-ext:atomic-push fiber (fsg-fibers target))
     ;; Pick a random scheduler and push to its PENDING queue (not the
     ;; deque directly).  The Chase-Lev deque only supports owner push;
     ;; this call may come from a non-carrier thread (e.g., hunchentoot's
     ;; accept thread), so we must use the thread-safe pending list.
     ;; The carrier's scheduler loop drains pending-fibers into the deque.
     (let* ((schedulers (fsg-schedulers target))
            (sched (aref schedulers (random (length schedulers)))))
       (setf (fiber-scheduler fiber) sched
             (fiber-state fiber) :runnable)
       (sb-ext:atomic-push fiber (fiber-scheduler-pending-fibers sched))))))

(defun try-steal-fiber (scheduler)
  "Try to steal a fiber from a sibling scheduler in the group.
Returns the stolen fiber, or NIL."
  (let ((group (fiber-scheduler-group scheduler)))
    (when group
      (let* ((schedulers (fsg-schedulers group))
             (n (length schedulers))
             (start (random n)))
        (loop for i from 0 below n
              for idx = (mod (+ start i) n)
              for victim = (aref schedulers idx)
              unless (eq victim scheduler)
                  do (let ((stolen (wsd-steal (fiber-scheduler-run-deque victim))))
                   (when stolen (return stolen))))))))

#+linux
(defun %scheduler-index-io-waiter (scheduler fiber)
  "Index FIBER in SCHEDULER's epoll waiter table by its wait-fd."
  (let* ((fd (fiber-wait-fd fiber))
         (table (fiber-scheduler-io-waiters scheduler)))
    (setf (gethash fd table) (cons fiber (gethash fd table))
          (fiber-io-indexed-p fiber) t)
    (incf (fiber-scheduler-io-waiter-count scheduler))))

#+linux
(defun %scheduler-remove-io-waiter (scheduler fiber)
  "Remove FIBER from SCHEDULER's io-waiters table."
  (let* ((fd (fiber-wait-fd fiber))
         (table (fiber-scheduler-io-waiters scheduler))
         (waiters (gethash fd table)))
    (when waiters
      (let ((remaining (delete fiber waiters :count 1)))
        (if remaining
            (setf (gethash fd table) remaining)
            (remhash fd table)))
      (setf (fiber-io-indexed-p fiber) nil)
      (decf (fiber-scheduler-io-waiter-count scheduler)))))

#+linux
(defun %scheduler-wake-ready-io-waiters (scheduler deque)
  "Wake epoll-indexed fibers whose FDs were reported ready this tick."
  (let ((ready-fds (fiber-scheduler-ready-fds scheduler))
        (table (fiber-scheduler-io-waiters scheduler)))
    (maphash
     (lambda (fd ignored)
       (declare (ignore ignored))
       (let ((waiters (gethash fd table)))
         (when waiters
           (remhash fd table)
           (dolist (f waiters)
             ;; Skip stale entries defensively (e.g. if state changed early).
             (when (and (fiber-io-indexed-p f)
                        (eq (fiber-state f) :suspended))
               (when (>= (fiber-heap-index f) 0)
                 (%heap-remove scheduler f))
               (setf (fiber-io-indexed-p f) nil
                     (fiber-next-waiting f) nil
                     (fiber-state f) :runnable
                     (fiber-wake-condition f) nil)
               (decf (fiber-scheduler-io-waiter-count scheduler))
               (wsd-push deque f))))))
     ready-fds)))

(declaim (inline scheduler-has-waiters-p))
(defun scheduler-has-waiters-p (scheduler)
  (or (fiber-scheduler-waiting scheduler)
      (plusp (fiber-scheduler-deadline-heap-count scheduler))
      #+linux (plusp (fiber-scheduler-io-waiter-count scheduler))))

;;;; ===== Per-scheduler deadline min-heap =====
;;;
;;; Binary min-heap ordered by fiber-deadline.  Each fiber's heap-index
;;; slot enables O(log N) removal.  Used to avoid O(W) generic list
;;; scans for deadline checks.

(declaim (inline %heap-parent %heap-left %heap-right))
(defun %heap-parent (i) (ash (1- i) -1))
(defun %heap-left (i) (1+ (ash i 1)))
(defun %heap-right (i) (+ 2 (ash i 1)))

(defun %heap-sift-up (heap i)
  "Sift heap[i] upward until heap property is restored."
  (let* ((fiber (svref heap i))
         (dl (fiber-deadline fiber)))
    (loop while (> i 0) do
      (let* ((parent-idx (%heap-parent i))
             (parent (svref heap parent-idx)))
        (when (<= (fiber-deadline parent) dl) (return))
        (setf (svref heap i) parent
              (fiber-heap-index parent) i
              i parent-idx)))
    (setf (svref heap i) fiber
          (fiber-heap-index fiber) i)))

(defun %heap-sift-down (heap count i)
  "Sift heap[i] downward until heap property is restored."
  (let* ((fiber (svref heap i))
         (dl (fiber-deadline fiber)))
    (loop
      (let ((left (%heap-left i))
            (smallest i)
            (smallest-dl dl))
        (when (and (< left count)
                   (< (fiber-deadline (svref heap left)) smallest-dl))
          (setf smallest left
                smallest-dl (fiber-deadline (svref heap left))))
        (let ((right (%heap-right i)))
          (when (and (< right count)
                     (< (fiber-deadline (svref heap right)) smallest-dl))
            (setf smallest right)))
        (when (= smallest i) (return))
        (let ((child (svref heap smallest)))
          (setf (svref heap i) child
                (fiber-heap-index child) i
                i smallest))))
    (setf (svref heap i) fiber
          (fiber-heap-index fiber) i)))

(defun %heap-insert (scheduler fiber)
  "Insert FIBER into SCHEDULER's deadline heap."
  (let* ((heap (fiber-scheduler-deadline-heap scheduler))
         (n (fiber-scheduler-deadline-heap-count scheduler)))
    (when (>= n (length heap))
      (let ((new-heap (make-array (* 2 (length heap)))))
        (replace new-heap heap)
        (setf heap new-heap
              (fiber-scheduler-deadline-heap scheduler) new-heap)))
    (setf (svref heap n) fiber
          (fiber-heap-index fiber) n)
    (incf (fiber-scheduler-deadline-heap-count scheduler))
    (%heap-sift-up heap n)))

(defun %heap-remove (scheduler fiber)
  "Remove FIBER from SCHEDULER's deadline heap by its heap-index."
  (let ((i (fiber-heap-index fiber)))
    (when (< i 0) (return-from %heap-remove))
    (setf (fiber-heap-index fiber) -1)
    (let* ((heap (fiber-scheduler-deadline-heap scheduler))
           (n (decf (fiber-scheduler-deadline-heap-count scheduler))))
      (when (= i n)
        (setf (svref heap n) nil)
        (return-from %heap-remove))
      (let ((last (svref heap n)))
        (setf (svref heap n) nil
              (svref heap i) last
              (fiber-heap-index last) i)
        (if (and (> i 0)
                 (< (fiber-deadline last)
                    (fiber-deadline (svref heap (%heap-parent i)))))
            (%heap-sift-up heap i)
            (%heap-sift-down heap n i))))))

(defun %heap-pop-expired (scheduler now deque)
  "Pop all expired fibers from the deadline heap and push to run deque.
Does NOT clear fiber-next-waiting -- the fiber may also be on the generic
waiting list (deadline+predicate case), and the intrusive list walk needs
the pointer intact to continue traversal."
  (let ((heap (fiber-scheduler-deadline-heap scheduler)))
    (loop while (plusp (fiber-scheduler-deadline-heap-count scheduler))
          for fiber = (svref heap 0)
          while (<= (fiber-deadline fiber) now)
          do
      (%heap-remove scheduler fiber)
      #+linux
      (when (fiber-io-indexed-p fiber)
        (%scheduler-remove-io-waiter scheduler fiber))
      (setf (fiber-state fiber) :runnable
            (fiber-wake-condition fiber) nil)
      (wsd-push deque fiber))))

#+linux
(defun %scheduler-harvest-epoll (scheduler timeout-ms)
  "Run epoll_wait once with TIMEOUT-MS, populate ready-fds hash table.
With EPOLLET|EPOLLONESHOT, each fd fires at most once per arm cycle."
  (let ((efd (fiber-scheduler-event-fd scheduler))
        (ready-fds (fiber-scheduler-ready-fds scheduler)))
    (when (minusp efd) (return-from %scheduler-harvest-epoll 0))
    (clrhash ready-fds)
    (let ((buf (fiber-scheduler-epoll-buf scheduler)))
      (sb-sys:with-pinned-objects (buf)
        (let* ((sap (sb-sys:vector-sap buf))
               (n (sb-unix:epoll-wait efd sap 64 timeout-ms)))
          (when (or (null n) (<= n 0))
            (return-from %scheduler-harvest-epoll 0))
          (dotimes (i n)
            (setf (gethash (aref buf i) ready-fds) t))
          n)))))

(defun run-fiber-scheduler (scheduler)
  "Run the scheduler loop on the current thread. Returns when all
fibers have completed."
  (setf (fiber-scheduler-carrier scheduler) *current-thread*)
  ;; Install the C trampoline if not done yet
  (install-fiber-trampoline)
  (let ((*current-scheduler* scheduler)
        (*current-fiber* nil)
        (deque (fiber-scheduler-run-deque scheduler))
        (group (fiber-scheduler-group scheduler)))
    (unwind-protect
         (loop
           ;; Drain pending-fibers queue (submitted by external threads)
           ;; into the local deque.  Uses CAS to atomically grab the list.
           (let ((pending (loop for old = (fiber-scheduler-pending-fibers scheduler)
                                when (null old) return nil
                                when (eq (sb-ext:cas
                                          (fiber-scheduler-pending-fibers scheduler)
                                          old nil)
                                         old)
                                return old)))
             (dolist (f (nreverse pending))
               (wsd-push deque f)))
           ;; Check waiting fibers for wake conditions
           ;; Capture time once per tick to avoid redundant vDSO calls.
           (let ((now (get-internal-real-time))
                 (still-head nil)
                 (still-tail nil))
             #+linux
             (progn
               (%scheduler-harvest-epoll scheduler 0)
               (when (plusp (fiber-scheduler-io-waiter-count scheduler))
                 (%scheduler-wake-ready-io-waiters scheduler deque)))
             ;; Pop expired deadline entries from heap
             (when (plusp (fiber-scheduler-deadline-heap-count scheduler))
               (%heap-pop-expired scheduler now deque))
             ;; Walk intrusive waiting list; capture next before modifying
             (loop for f = (fiber-scheduler-waiting scheduler) then next
                   for next = (when f (fiber-next-waiting f))
                   while f do
               (block check-fiber
                 ;; Skip fibers already woken by io-waiter wake or heap expiry
                 (unless (eq (fiber-state f) :suspended)
                   (return-from check-fiber))
                 (let ((wake nil))
                   ;; Check deadline using captured 'now'
                   (let ((dl (fiber-deadline f)))
                     (when (and dl (>= now dl))
                       (setf wake t)))
                   ;; Non-I/O waiters, or non-Linux, or no epoll: call wake-condition
                   (unless wake
                     (when (and (fiber-wake-condition f)
                                (funcall (fiber-wake-condition f)))
                       (setf wake t)))
                   (if wake
                       (progn
                         (when (>= (fiber-heap-index f) 0)
                           (%heap-remove scheduler f))
                         (setf (fiber-next-waiting f) nil
                               (fiber-state f) :runnable
                               (fiber-wake-condition f) nil)
                         (wsd-push deque f))
                       (progn
                         ;; Link into still-waiting intrusive list
                         (setf (fiber-next-waiting f) nil)
                         (if still-tail
                             (setf (fiber-next-waiting still-tail) f)
                             (setf still-head f))
                         (setf still-tail f))))))
             (setf (fiber-scheduler-waiting scheduler) still-head))
           ;; Pick next fiber: local pop, then try stealing
           (let ((fiber (or (wsd-pop deque)
                            (try-steal-fiber scheduler))))
             (cond
               (fiber
                ;; Update scheduler back-pointer and carrier
                (setf (fiber-scheduler fiber) scheduler
                      (fiber-carrier fiber) *current-thread*)
                (setf (fiber-scheduler-current-fiber scheduler) fiber)
                (ecase (fiber-state fiber)
                  (:created  (start-fiber scheduler fiber))
                  (:runnable (resume-fiber scheduler fiber)))
                ;; Fiber yielded or finished
                (setf (fiber-scheduler-current-fiber scheduler) nil)
                ;; Handle post-switch state
                (case (fiber-state fiber)
                  (:suspended
                   (let ((has-fd (>= (fiber-wait-fd fiber) 0))
                         (has-deadline (fiber-deadline fiber))
                         (has-wake (fiber-wake-condition fiber)))
                     (cond
                       ;; No wake reason → back to deque
                       ((not (or has-wake has-fd has-deadline))
                        (setf (fiber-state fiber) :runnable)
                        (wsd-push deque fiber))
                       ;; fd waiter on Linux with epoll → io-waiters (+ heap if deadline)
                       #+linux
                       ((and has-fd (plusp (fiber-scheduler-event-fd scheduler)))
                        (%scheduler-index-io-waiter scheduler fiber)
                        (when has-deadline
                          (%heap-insert scheduler fiber)))
                       ;; Pure deadline, no predicate (fiber-sleep) → heap only
                       ((and has-deadline (not has-wake))
                        (%heap-insert scheduler fiber))
                       ;; Deadline + something else → heap + generic list
                       (has-deadline
                        (%heap-insert scheduler fiber)
                        (setf (fiber-next-waiting fiber)
                              (fiber-scheduler-waiting scheduler))
                        (setf (fiber-scheduler-waiting scheduler) fiber))
                       ;; Everything else → generic list
                       (t
                        (setf (fiber-next-waiting fiber)
                              (fiber-scheduler-waiting scheduler))
                        (setf (fiber-scheduler-waiting scheduler) fiber)))))
                  (:dead
                   ;; Decrement group active count (CAS loop)
                   (when group
                     (loop for old = (fsg-active-count group)
                           until (eq (sb-ext:cas (fsg-active-count group)
                                                 old (1- old))
                                     old)))
                   ;; Clean up dead fiber resources
                   (destroy-fiber fiber))))
               ;; No runnable fibers but some waiting
               ((scheduler-has-waiters-p scheduler)
                (let ((hook (fiber-scheduler-idle-hook scheduler)))
                  (if hook
                      (funcall hook scheduler)
                      (fiber-io-idle-hook scheduler))))
               ;; No local work and no waiting; check group
               (group
                (if (plusp (the fixnum (fsg-active-count group)))
                    ;; Other carriers still have active fibers; brief yield then retry
                    (sb-unix:nanosleep 0 100000)  ; 100us
                    ;; All fibers globally done
                    (return)))
               ;; Single-carrier mode: all done
               (t (return)))))
      ;; Cleanup: close event multiplexer fd
      #+(or linux bsd)
      (let ((efd (fiber-scheduler-event-fd scheduler)))
        (when (plusp efd)
          (sb-unix:unix-close efd)
          (setf (fiber-scheduler-event-fd scheduler) -1))))))

;;;; ===== Convenience API =====

(defun %always-false () nil)

(defun fiber-sleep (seconds)
  "Sleep the current fiber for SECONDS (can be fractional).
Only works within a fiber context.  Uses the deadline slot and a global
sentinel instead of allocating a closure."
  (let ((deadline (+ (get-internal-real-time)
                     (truncate (* seconds internal-time-units-per-second)))))
    (setf (fiber-deadline *current-fiber*) deadline)
    (fiber-yield nil)
    (setf (fiber-deadline *current-fiber*) nil)))

(defun %fiber-sleep (seconds)
  "Fiber dispatch for CL:SLEEP. Returns :PINNED-FALL-THROUGH if pinned."
  (unless (fiber-can-yield-p)
    (check-pinned-blocking 'sleep)
    (return-from %fiber-sleep :pinned-fall-through))
  (fiber-sleep seconds)
  nil)

(defun %fiber-wait-for (test stop-sec stop-usec)
  "Fiber dispatch for SB-EXT:WAIT-FOR. Returns :PINNED-FALL-THROUGH if pinned."
  (declare (function test))
  (unless (fiber-can-yield-p)
    (check-pinned-blocking 'wait-for)
    (return-from %fiber-wait-for :pinned-fall-through))
  (let ((timeout
          (when stop-sec
            (let ((remaining-usec
                    (- (+ (* 1000000 stop-sec) stop-usec)
                       (multiple-value-bind (sec usec)
                           (decode-internal-time (get-internal-real-time))
                         (+ (* 1000000 sec) usec)))))
              (when (plusp remaining-usec)
                (/ remaining-usec 1000000.0d0))))))
    (if (and timeout (not (plusp timeout)))
        nil  ; already timed out
        (fiber-park test :timeout timeout))))

(defun fiber-alive-p (fiber)
  "Return true if the fiber has not yet finished."
  (not (eq (fiber-state fiber) :dead)))

(declaim (inline %relative-decoded-times))
(defun %relative-decoded-times (abs-sec abs-usec)
  "Return remaining decoded time until ABS-SEC/ABS-USEC as two values."
  (multiple-value-bind (now-sec now-usec)
      (decode-internal-time (get-internal-real-time))
    (let ((remaining-usec (- (+ (* 1000000 abs-sec) abs-usec)
                             (+ (* 1000000 now-sec) now-usec))))
      (if (plusp remaining-usec)
          (values (truncate remaining-usec 1000000)
                  (mod remaining-usec 1000000))
          (values 0 0)))))

;;;; ===== Pinned-blocking check =====

(defvar *pinned-blocking-action* :warn
  "Action when a pinned fiber hits a blocking primitive.
:WARN emits a warning, :ERROR signals an error, NIL does nothing.")

(defun check-pinned-blocking (operation)
  "Check-and-signal when a pinned fiber attempts to block on OPERATION."
  (let ((fiber *current-fiber*))
    (case *pinned-blocking-action*
      (:warn  (warn "Fiber ~A is pinned, blocking carrier on ~A"
                    (fiber-name fiber) operation))
      (:error (error "Fiber ~A is pinned, cannot yield for ~A"
                     (fiber-name fiber) operation)))))

;;;; ===== fiber-park: general-purpose park with timeout =====

(defun fiber-park (predicate &key timeout)
  "Park current fiber until PREDICATE returns true or TIMEOUT (seconds) expires.
Returns T if predicate satisfied, NIL on timeout.  Passes the raw predicate
to fiber-yield; deadline is stored in the fiber struct and checked by the
scheduler loop, avoiding a wrapper closure allocation."
  (let ((deadline (when timeout
                    (+ (get-internal-real-time)
                       (truncate (* timeout internal-time-units-per-second))))))
    ;; Store deadline in fiber struct for scheduler fast-path checking
    (when deadline
      (setf (fiber-deadline *current-fiber*) deadline))
    (fiber-yield predicate)
    ;; Clear deadline after wake
    (when deadline
      (setf (fiber-deadline *current-fiber*) nil))
    ;; After wake: check which condition triggered
    (if (and deadline (>= (get-internal-real-time) deadline)
             (not (funcall predicate)))
        nil   ; timeout
        t)))  ; predicate satisfied

;;;; ===== fiber-join =====

(defun fiber-join (target &key timeout)
  "Wait until TARGET fiber completes and return its result, or NIL on timeout.
Works from both fiber context (parks the fiber) and OS thread context
(blocks the thread via a waitqueue)."
  (when (eq target (current-fiber))
    (error "Fiber cannot join itself"))
  (cond
    ;; Already dead — just return result
    ((eq (fiber-state target) :dead)
     (fiber-result target))
    ;; In fiber context — park until target dies
    ((and *current-fiber* *current-scheduler*)
     (fiber-park (lambda () (eq (fiber-state target) :dead)) :timeout timeout)
     (if (eq (fiber-state target) :dead)
         (fiber-result target)
         nil))
    ;; OS thread context — spin-wait with brief sleeps
    (t
     (let ((deadline (when timeout
                       (+ (get-internal-real-time)
                          (truncate (* timeout internal-time-units-per-second))))))
       (loop
         (when (eq (fiber-state target) :dead)
           (return (fiber-result target)))
         (when (and deadline (>= (get-internal-real-time) deadline))
           (return nil))
         (sb-unix:nanosleep 0 1000000))))))

;;;; ===== Fiber-aware threading primitives =====

(defun %fiber-condition-wait (queue mutex timeout stop-sec stop-usec)
  "Fiber-aware condition-wait. Release MUTEX, park fiber until notified,
re-acquire MUTEX. Returns T (or multiple values with remaining time)
on success, NIL on timeout. Returns :PINNED-FALL-THROUGH when fiber
is pinned (caller should use the OS blocking path)."
  (unless (fiber-can-yield-p)
    (check-pinned-blocking 'condition-wait)
    (return-from %fiber-condition-wait :pinned-fall-through))
  (let ((gen (waitqueue-fiber-generation queue)))
    (release-mutex mutex)
    (let ((notified (fiber-park
                     (lambda () (/= (waitqueue-fiber-generation queue) gen))
                     :timeout timeout)))
      (if (not notified)
          nil  ; timeout during wait
          ;; Notified -- re-acquire mutex with remaining time
          (let ((remaining-secs
                  (when stop-sec
                    (multiple-value-bind (sec usec)
                        (%relative-decoded-times stop-sec stop-usec)
                      (when (and (zerop sec) (not (plusp usec)))
                        (return-from %fiber-condition-wait nil))
                      (+ sec (/ usec 1000000.0d0))))))
            (unless (or (%try-mutex mutex)
                        (%fiber-grab-mutex-internal mutex remaining-secs))
              (return-from %fiber-condition-wait nil))
            ;; Success: return T, or (values T remaining-sec remaining-usec)
            (if stop-sec
                (multiple-value-bind (sec usec)
                    (%relative-decoded-times stop-sec stop-usec)
                  (values t sec usec))
                t))))))

(defun %fiber-grab-mutex-internal (mutex timeout)
  "Internal fiber-aware mutex acquisition (no pin check).
Returns T on success, NIL on timeout."
  (let ((pred (lambda ()
                #+sb-futex (eql (mutex-state mutex) 0)
                #-sb-futex (eql (mutex-%owner mutex) 0))))
    (loop
      (let ((ok (fiber-park pred :timeout timeout)))
        (unless ok (return nil))           ; timeout
        (when (%try-mutex mutex) (return t))))))

(defun %fiber-grab-mutex (mutex timeout)
  "Fiber-aware mutex acquisition. Yield until mutex is free, then CAS.
Returns T on success, NIL on timeout. Returns :PINNED-FALL-THROUGH
when fiber is pinned (caller should use the OS blocking path)."
  (unless (fiber-can-yield-p)
    (check-pinned-blocking 'grab-mutex)
    (return-from %fiber-grab-mutex :pinned-fall-through))
  (%fiber-grab-mutex-internal mutex timeout))

;;;; ===== Fiber-aware I/O =====

(defun fd-ready-p (fd direction)
  "Non-blocking check: is FD usable for DIRECTION (:input or :output)?"
  #+win32
  (if (eq direction :output)
      t  ; Windows always reports output as ready
      (sb-win32:handle-listen fd 0 0))
  #-win32
  (sb-unix:unix-simple-poll fd direction 0))

(defun %fiber-wait-until-fd-usable (fd direction timeout)
  "Fiber-aware fd wait. Park fiber until FD is usable for DIRECTION.
Returns T on success, NIL on timeout. Returns :PINNED-FALL-THROUGH
when fiber is pinned."
  (unless (fiber-can-yield-p)
    (check-pinned-blocking 'wait-until-fd-usable)
    (return-from %fiber-wait-until-fd-usable :pinned-fall-through))
  ;; Keep timeout=0 behavior non-blocking.
  (when (and timeout (not (plusp timeout)))
    (return-from %fiber-wait-until-fd-usable
      #+linux
      (let ((scheduler *current-scheduler*))
        (if (and scheduler
                 (plusp (fiber-scheduler-event-fd scheduler))
                 (gethash fd (fiber-scheduler-ready-fds scheduler)))
            t
            (fd-ready-p fd direction)))
      #-linux
      (fd-ready-p fd direction)))
  ;; Park with fd-readiness wait-info, using epoll-driven wake when available.
  (let ((fiber *current-fiber*)
        (scheduler *current-scheduler*))
    (setf (fiber-wait-fd fiber) fd
          (fiber-wait-direction fiber) direction)
    (%event-register scheduler fd direction)
    (unwind-protect
         #+linux
         (if (plusp (fiber-scheduler-event-fd scheduler))
             ;; With epoll, avoid per-wake poll() checks and rely on scheduler
             ;; wakeup + optional deadline handling.
             (let ((deadline (when timeout
                               (+ (get-internal-real-time)
                                  (truncate (* timeout
                                               internal-time-units-per-second))))))
               (when deadline
                 (setf (fiber-deadline fiber) deadline))
               (fiber-yield #'%always-false)
               (when deadline
                 (setf (fiber-deadline fiber) nil))
               (if (and deadline
                        (>= (get-internal-real-time) deadline)
                        (not (fd-ready-p fd direction)))
                   nil
                   t))
             (let ((result (fiber-park (lambda () (fd-ready-p fd direction))
                                       :timeout timeout)))
               (if result t nil)))
         #-linux
         (let ((result (fiber-park (lambda () (fd-ready-p fd direction))
                                   :timeout timeout)))
           (if result t nil))
      (setf (fiber-wait-fd fiber) -1))))

;;;; ===== Event multiplexer registration (epoll/kqueue) =====

#+(or linux bsd)
(defun %event-register (scheduler fd direction)
  "Register fd/direction with the scheduler's event multiplexer."
  (let ((efd (fiber-scheduler-event-fd scheduler)))
    (when (minusp efd) (return-from %event-register))
    #+linux
    (let* ((table (fiber-scheduler-registered-fds scheduler))
           (events (logior (if (eq direction :input)
                               sb-unix:epollin sb-unix:epollout)
                           sb-unix:epollet
                           sb-unix:epolloneshot))
           (current (gethash fd table 0)))
      (if (zerop current)
          (sb-unix:epoll-ctl-add efd fd events)
          ;; Re-arm: EPOLLONESHOT disables after one event, MOD re-enables
          (sb-unix:epoll-ctl-mod efd fd events))
      (setf (gethash fd table) events))
    #+bsd
    (%kqueue-register efd fd direction :add)))

#+(or linux bsd)
(defun %event-deregister (scheduler fd direction)
  "Deregister fd/direction from the scheduler's event multiplexer."
  (let ((efd (fiber-scheduler-event-fd scheduler)))
    (when (minusp efd) (return-from %event-deregister))
    #+linux
    (let* ((table (fiber-scheduler-registered-fds scheduler))
           (remove-events (if (eq direction :input)
                              sb-unix:epollin sb-unix:epollout))
           (current (gethash fd table 0))
           (remaining (logandc2 current remove-events)))
      ;; Check if any direction bits remain (ignore flag bits like EPOLLET)
      (if (zerop (logand remaining (logior sb-unix:epollin sb-unix:epollout)))
          (progn (sb-unix:epoll-ctl-del efd fd)
                 (remhash fd table))
          (progn (sb-unix:epoll-ctl-mod efd fd remaining)
                 (setf (gethash fd table) remaining))))
    #+bsd
    (%kqueue-register efd fd direction :delete)))

#-(or linux bsd)
(defun %event-register (scheduler fd direction)
  (declare (ignore scheduler fd direction)))
#-(or linux bsd)
(defun %event-deregister (scheduler fd direction)
  (declare (ignore scheduler fd direction)))

#+bsd
(defun %kqueue-register (kq fd direction action)
  "Add or delete a kevent filter for fd."
  (let ((filter (if (eq direction :input)
                    sb-unix:evfilt-read
                    sb-unix:evfilt-write))
        (flags (if (eq action :add)
                   (logior sb-unix:ev-add sb-unix:ev-enable)
                   sb-unix:ev-delete)))
    (with-alien ((kev (struct sb-unix:kevent)))
      (setf (slot kev 'sb-unix::ident) fd
            (slot kev 'sb-unix::filter) filter
            (slot kev 'sb-unix::flags) flags
            (slot kev 'sb-unix::fflags) 0
            (slot kev 'sb-unix::data) 0
            (slot kev 'sb-unix::udata) 0)
      ;; Register change, don't wait for events (nevents=0)
      (sb-unix:kevent kq (alien-sap (addr kev)) 1
                      (int-sap 0) 0 (int-sap 0)))))

;;;; ===== Efficient Idle Hook (batched I/O polling) =====

(defun compute-nearest-deadline (scheduler)
  "Return nearest deadline in milliseconds from the deadline heap, or NIL."
  (let ((count (fiber-scheduler-deadline-heap-count scheduler)))
    (when (plusp count)
      (let* ((fiber (svref (fiber-scheduler-deadline-heap scheduler) 0))
             (dl (fiber-deadline fiber))
             (now (get-internal-real-time)))
        (max 1 (truncate (* 1000 (max 0 (- dl now)))
                         internal-time-units-per-second))))))

;;; Shared fallback: register temporary handlers and call serve-event.
#-win32
(defun %batched-fd-poll/serve-event (scheduler timeout-ms)
  "Fallback: register temporary handlers and call serve-event."
  (let ((handlers nil)
        (timeout-sec (/ timeout-ms 1000.0d0)))
    (unwind-protect
         (progn
           (loop for f = (fiber-scheduler-waiting scheduler) then (fiber-next-waiting f)
                 while f do
             (let ((wait-fd (fiber-wait-fd f)))
               (when (>= wait-fd 0)
                 (push (add-fd-handler
                        wait-fd
                        (fiber-wait-direction f)
                        (lambda (fd) (declare (ignore fd))))
                       handlers))))
           (when handlers
             (serve-event timeout-sec)))
      (dolist (h handlers)
        (remove-fd-handler h)))))

;;; Platform-specific %batched-fd-poll implementations
#+linux
(defun %batched-fd-poll (scheduler timeout-ms)
  "Wait for I/O events using epoll, falling back to serve-event.
Populates the scheduler's ready-fds table and wakes io-waiters so
the caller (idle hook) can return to the scheduler loop with fibers
already in the run deque."
  (let ((efd (fiber-scheduler-event-fd scheduler)))
    (if (minusp efd)
        (%batched-fd-poll/serve-event scheduler timeout-ms)
        (progn
          (%scheduler-harvest-epoll scheduler (min timeout-ms 1000))
          (when (plusp (fiber-scheduler-io-waiter-count scheduler))
            (%scheduler-wake-ready-io-waiters
             scheduler (fiber-scheduler-run-deque scheduler)))))))

#+bsd
(defun %batched-fd-poll (scheduler timeout-ms)
  "Wait for I/O events using kqueue, falling back to serve-event."
  (let ((kq (fiber-scheduler-event-fd scheduler)))
    (if (minusp kq)
        (%batched-fd-poll/serve-event scheduler timeout-ms)
        (with-alien ((timeout (struct sb-unix::timespec)))
          (let ((sec (truncate timeout-ms 1000))
                (nsec (* (mod timeout-ms 1000) 1000000)))
            (setf (slot timeout 'sb-unix::tv-sec) sec
                  (slot timeout 'sb-unix::tv-nsec) nsec)
            (with-alien ((events (array (struct sb-unix:kevent) 64)))
              (sb-unix:kevent kq (int-sap 0) 0
                              (alien-sap (addr events)) 64
                              (alien-sap (addr timeout)))))))))

#-(or linux bsd win32)
(defun %batched-fd-poll (scheduler timeout-ms)
  (%batched-fd-poll/serve-event scheduler timeout-ms))

#+win32
(defun %batched-fd-poll (scheduler timeout-ms)
  "Poll each waiting fiber's fd on Windows."
  (let ((any-ready nil))
    (loop for f = (fiber-scheduler-waiting scheduler) then (fiber-next-waiting f)
          while f do
      (let ((wait-fd (fiber-wait-fd f)))
        (when (and (>= wait-fd 0)
                   (fd-ready-p wait-fd (fiber-wait-direction f)))
          (setf any-ready t))))
    (unless any-ready
      (sb-unix:nanosleep 0 (* (min timeout-ms 10) 1000000)))))

(defun fiber-io-idle-hook (scheduler)
  "Idle hook that polls fds from waiting fibers using a single poll/select call.
Called when no fibers are runnable but some are waiting."
  (let ((timeout-ms (or (compute-nearest-deadline scheduler) 100))
        #+linux
        (has-io-waiters (plusp (fiber-scheduler-io-waiter-count scheduler)))
        #-linux
        (has-io-waiters nil))
    ;; Check if any waiting fibers have I/O wait info
    (loop for f = (fiber-scheduler-waiting scheduler) then (fiber-next-waiting f)
          while f do
      (when (>= (fiber-wait-fd f) 0)
        (setf has-io-waiters t)
        (return)))
    (if has-io-waiters
        (%batched-fd-poll scheduler timeout-ms)
        ;; No I/O waiters; sleep briefly for timer-based waits
        (sb-unix:nanosleep 0 (* (min timeout-ms 10) 1000000)))))

;;;; ===== Multi-carrier scheduling =====

(defun %default-carrier-count ()
  "Return the number of available CPUs, respecting cgroup limits on Linux."
  (let ((online
          #+win32
          (deref (extern-alien "os_number_of_processors" int))
          #-win32
          (alien-funcall
           (extern-alien "sysconf" (function long int))
           #+linux 84    ; _SC_NPROCESSORS_ONLN
           #+darwin 58   ; _SC_NPROCESSORS_ONLN on macOS
           #-(or linux darwin) 84)))
    (when (< online 1) (setf online 1))
    #+linux
    (let ((cgroup (sb-sys::%cgroup-effective-cpus)))
      (when cgroup
        (setf online (min online cgroup))))
    (max 1 online)))

(defun start-fibers (fibers &key (carrier-count (%default-carrier-count)) idle-hook)
  "Start FIBERS across CARRIER-COUNT background carrier threads.
Returns a FIBER-SCHEDULER-GROUP handle immediately.  All carriers run on
background threads (even when CARRIER-COUNT is 1).  Use FINISH-FIBERS to
join the carriers and collect results, or FIBER-GROUP-DONE-P to poll."
  (let ((fibers (coerce fibers 'list)))
    (when (null fibers) (return-from start-fibers nil))
    (let* ((schedulers (loop repeat carrier-count
                             collect (make-fiber-scheduler :idle-hook idle-hook)))
           (sched-vec (coerce schedulers 'simple-vector))
           (group (%make-fiber-scheduler-group
                   :schedulers sched-vec
                   :active-count (length fibers)
                   :fibers fibers)))
      ;; Link schedulers to group
      (loop for s across sched-vec
            for i from 0
            do (setf (fiber-scheduler-group s) group
                     (fiber-scheduler-index s) i))
      ;; Distribute fibers round-robin
      (loop for f in fibers
            for i from 0
            do (submit-fiber (aref sched-vec (mod i carrier-count)) f))
      ;; Spawn ALL carriers as background threads
      (let ((threads nil))
        (dolist (sched schedulers)
          (push (make-thread (lambda () (run-fiber-scheduler sched))
                             :name "fiber-carrier")
                threads))
        (setf (fsg-threads group) (nreverse threads)))
      group)))

(defun finish-fibers (group)
  "Join all carrier threads in GROUP and return a list of fiber results.
Blocks until all fibers have completed."
  (when (null group) (return-from finish-fibers nil))
  (dolist (th (fsg-threads group))
    (join-thread th))
  (mapcar #'fiber-result (fsg-fibers group)))

(defun fiber-group-done-p (group)
  "Return T if all fibers in GROUP have completed (non-blocking check)."
  (if (null group)
      t
      (zerop (the fixnum (fsg-active-count group)))))

(defun run-fibers (fibers &key (carrier-count (%default-carrier-count)) idle-hook)
  "Run FIBERS across CARRIER-COUNT carrier threads.  Returns a list of
fiber results when all fibers have completed.
Convenience wrapper around START-FIBERS + FINISH-FIBERS."
  (if (null fibers)
      nil
      (finish-fibers (start-fibers fibers :carrier-count carrier-count
                                          :idle-hook idle-hook))))

;;;; ===== Fiber Backtrace =====

(defun fiber-get-backtrace (fiber)
  "Return a backtrace list for a suspended fiber.
Each element is either (CODE . OFFSET) for Lisp frames, or a raw PC
for foreign frames.  Only works for :SUSPENDED or :CREATED fibers."
  (unless (member (fiber-state fiber) '(:suspended :created))
    (error "Can only get backtrace for suspended/created fibers, not ~S"
           (fiber-state fiber)))
  (let* ((saved-rsp (fiber-saved-rsp fiber))
         (stack-start (fiber-control-stack-start fiber))
         (stack-end (fiber-control-stack-end fiber))
         ;; Extract FP and PC from the fiber-switch save area.
         ;; The save area layout is architecture-specific:
         ;;   x86-64 SysV: 6 pushed regs, FP(rbp) at +40, ret addr at +48
         ;;   x86-64 Win64: 8 pushed regs, FP(rbp) at +56, ret addr at +64
         ;;   ARM64: stp x29,x30 first, FP at +0, LR at +8
         ;;   ARM32: r11 at +96, lr at +100
         ;;   PPC64: backchain+0, LR at +8, r15(cfp) at +32
         (fp #+(and x86-64 (not win32))
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 40)
             #+(and x86-64 win32)
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 56)
             #+arm64
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 0)
             #+arm
             (sb-sys:sap-ref-32 (sb-sys:int-sap saved-rsp) 96)
             #+ppc64
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 32))
         (pc #+(and x86-64 (not win32))
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 48)
             #+(and x86-64 win32)
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 64)
             #+arm64
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 8)
             #+arm
             (sb-sys:sap-ref-32 (sb-sys:int-sap saved-rsp) 100)
             #+ppc64
             (sb-sys:sap-ref-word (sb-sys:int-sap saved-rsp) 8))
         (list))
    ;; Walk the FP chain, collecting (code . offset) or raw PCs
    (flet ((store-pc (pc)
             (let ((code (sb-di::code-header-from-pc pc)))
               (push (if code
                         (cons code (- pc
                                      (sb-sys:sap-int
                                       (sb-kernel:code-instructions code))))
                         pc)
                     list))))
      (store-pc pc)
      (when (and (< stack-start fp stack-end) (> fp saved-rsp))
        (loop
         (let ((next-fp (sb-sys:sap-ref-word (sb-sys:int-sap fp) 0))
               (next-pc (sb-sys:sap-ref-word (sb-sys:int-sap fp) 8)))
           (store-pc next-pc)
           (unless (and (> next-fp fp) (< next-fp stack-end)) (return))
           (setq fp next-fp)))))
    (nreverse list)))

;;;; ===== Exports =====

(export '(make-fiber
          fiber
          fiber-name
          fiber-state
          fiber-result
          fiber-alive-p
          fiber-yield
          fiber-sleep
          fiber-park
          fiber-join
          fiber-pin
          fiber-unpin
          fiber-can-yield-p
          with-fiber-pinned
          *pinned-blocking-action*
          current-fiber
          submit-fiber
          run-fibers
          start-fibers
          finish-fibers
          fiber-group-done-p
          fiber-scheduler-group
          *current-fiber*
          *current-scheduler*
          list-all-fibers
          fiber-get-backtrace))

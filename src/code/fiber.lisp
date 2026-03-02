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

(define-alien-routine "free_fiber_binding_stack"
    void (stack system-area-pointer))

(define-alien-routine "make_fiber_gc_info"
    system-area-pointer)

(define-alien-routine "register_fiber_for_gc"
    void (info system-area-pointer))

(define-alien-routine "unregister_fiber_for_gc"
    void (info system-area-pointer))

(define-alien-routine "free_fiber_gc_info"
    void (info system-area-pointer))


;;; Persistent carrier fiber context (lock-free hot path)
(define-alien-routine "init_carrier_fiber_context" void)
(define-alien-routine "destroy_carrier_fiber_context" void)
(define-alien-routine "update_fiber_gc_context"
    void
  (fiber-stack-start unsigned-long)
  (fiber-stack-end unsigned-long)
  (fiber-bs-start unsigned-long)
  (carrier-bsp unsigned-long))
(define-alien-routine "clear_fiber_gc_context" void)

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
  (errorp nil :type boolean)  ; T if result is from an unhandled error
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
  (fibers nil :type t)
  ;; Carrier parking: idle carriers block on park-cv instead of spinning
  (park-lock (sb-thread:make-mutex :name "carrier-park") :type sb-thread:mutex)
  (park-cv (sb-thread:make-waitqueue :name "carrier-park") :type sb-thread:waitqueue)
  ;; Set to T when shutdown begins; submit-fiber rejects late submissions
  (closed-p nil :type boolean))

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
          (setf (fiber-saved-rsp fiber) base)))

      #+(and ppc (not ppc64))
      ;; PPC32: 240-byte frame.
      ;; fiber-switch saves: LR, CR, r14-r31 (18 GPRs), f14-f31 (18 FPRs).
      ;; Initial frame has trampoline in LR slot, fiber_lispobj in r15 (cfp) slot.
      ;;
      ;; Frame layout (offsets from frame base = saved SP):
      ;;   +0:   backchain (0, 4 bytes)
      ;;   +4:   LR = trampoline_addr (4 bytes)
      ;;   +8:   CR = 0 (4 bytes)
      ;;   +12:  r14 = 0 (bsp)
      ;;   +16:  r15 = fiber_lispobj (cfp, used by trampoline)
      ;;   +20..+80: r16-r31 = 0
      ;;   +84:  padding (4 bytes for FPR alignment)
      ;;   +88..+224: f14-f31 = 0
      ;;   +232..+239: padding
      ;;   saved SP = stack_top - 240
      (let ((base (- stack-top 240)))
        (let ((base-sap (sb-sys:int-sap base)))
          ;; Backchain, LR, CR
          (setf (sb-sys:sap-ref-32 base-sap 0) 0)               ; backchain
          (setf (sb-sys:sap-ref-32 base-sap 4) trampoline-addr) ; LR
          (setf (sb-sys:sap-ref-32 base-sap 8) 0)               ; CR
          ;; GPRs r14-r31 (18 regs at offsets 12..80, 4 bytes each)
          (loop for offset from 12 to 80 by 4
                do (setf (sb-sys:sap-ref-32 base-sap offset) 0))
          ;; r15 (cfp) = fiber lispobj, at offset 16
          (setf (sb-sys:sap-ref-32 base-sap 16) fiber-lispobj)
          ;; Padding at +84
          (setf (sb-sys:sap-ref-32 base-sap 84) 0)
          ;; FPRs f14-f31 (18 regs at offsets 88..224, 8 bytes each)
          (loop for offset from 88 to 224 by 8
                do (setf (sb-sys:sap-ref-double base-sap offset) 0.0d0))
          (setf (fiber-saved-rsp fiber) base)))

      #+riscv
      ;; RISC-V: 208-byte frame.
      ;; fiber-switch saves: ra, cfp (for debugger), s0-s11, fs0-fs11.
      ;; Initial frame has trampoline in ra slot, fiber_lispobj in cfp slot.
      ;;
      ;; Frame layout (offsets from frame base = saved SP):
      ;;   +0:   ra = trampoline_addr (8 bytes)
      ;;   +8:   cfp = fiber_lispobj (8 bytes, for trampoline)
      ;;   +16..+104: s0-s11 = 0 (12 GPRs × 8 = 96 bytes)
      ;;   +112..+200: fs0-fs11 = 0 (12 FPRs × 8 = 96 bytes)
      ;;   saved SP = stack_top - 208
      (let ((base (- stack-top 208)))
        (let ((base-sap (sb-sys:int-sap base)))
          ;; ra and cfp
          (setf (sb-sys:sap-ref-word base-sap 0) trampoline-addr) ; ra
          (setf (sb-sys:sap-ref-word base-sap 8) fiber-lispobj)   ; cfp
          ;; GPRs s0-s11 (12 regs at offsets 16..104)
          (loop for offset from 16 to 104 by 8
                do (setf (sb-sys:sap-ref-word base-sap offset) 0))
          ;; FPRs fs0-fs11 (12 regs at offsets 112..200)
          (loop for offset from 112 to 200 by 8
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
             (bstack-guard-size (sb-sys:sap-ref-word bstack-sap (* 2 sb-vm:n-word-bytes)))
             ;; Usable binding stack: guard page is at the top (grows upward)
             (bstack-usable-size (- bstack-total-size bstack-guard-size))
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
                     :binding-stack-size bstack-usable-size
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
              (free-fiber-binding-stack bstack-sap)
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
      (free-fiber-binding-stack bstack)
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
        ;; 1c. Update gc_info's binding-stack-pointer NOW, before we
        ;;     restore the carrier's BSP (step 3).  After step 3 the
        ;;     thread's BSP is in the carrier's range, so the
        ;;     active_fiber_context path won't cover the fiber's binding
        ;;     stack.  GC can fire here (without-interrupts does NOT
        ;;     prevent GC), so the gc_info must cover the full binding
        ;;     stack to avoid missing entries during precise scavenging.
        (let ((gc-info (fiber-gc-info fiber)))
          (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
            (sb-sys:without-gcing
              (setf (sb-sys:sap-ref-word gc-info (* 4 sb-vm:n-word-bytes))
                    (fiber-binding-stack-pointer fiber)))))
        ;; 2. Save fiber's TLS values and restore carrier's TLS values.
        ;;    Skip entirely when fiber has no special variable bindings
        ;;    (BSP = binding-stack-start), avoiding hash table + array overhead.
        (when (/= (fiber-binding-stack-pointer fiber)
                  (fiber-binding-stack-start fiber))
          (save-fiber-tls-and-restore-carrier fiber sched))
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
      ;;   lispobj* binding_stack_end;      // offset 40
      ;;   ... next/prev pointers           // offset 48, 56
      ;; }
      (setf (sb-sys:sap-ref-word gc-info 0) (fiber-control-stack-start fiber))
      (setf (sb-sys:sap-ref-word gc-info sb-vm:n-word-bytes) (fiber-saved-rsp fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 2 sb-vm:n-word-bytes)) (fiber-control-stack-end fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 3 sb-vm:n-word-bytes)) (fiber-binding-stack-start fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 4 sb-vm:n-word-bytes)) (fiber-binding-stack-pointer fiber))
      (setf (sb-sys:sap-ref-word gc-info (* 5 sb-vm:n-word-bytes))
            (+ (fiber-binding-stack-start fiber) (fiber-binding-stack-size fiber)))
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

(defun resume-fiber (scheduler fiber &optional migrated)
  "Resume a suspended fiber. MIGRATED is true if fiber changed carriers."
  (setf (fiber-state fiber) :running)
  (resume-fiber-internal scheduler fiber migrated))

(defun resume-fiber-internal (scheduler fiber &optional migrated)
  "Common resume path for both new and suspended fibers.
MIGRATED is true if the fiber changed carriers (work stealing)."
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
               (ash sb-vm::thread-control-stack-end-slot sb-vm:word-shift))))
      ;; 1. Save carrier's BSP, catch block, and UWP block
      (setf (fiber-scheduler-carrier-bsp scheduler) carrier-bsp)
      (setf (fiber-scheduler-carrier-catch-block scheduler)
            (sb-sys:sap-ref-word thread-sap catch-offset))
      (setf (fiber-scheduler-carrier-unwind-protect-block scheduler)
            (sb-sys:sap-ref-word thread-sap uwp-offset))
      ;; 2. Update GC context: tell GC about the fiber's stack
      ;;    boundaries and update carrier's suspended stack range.
      ;;    No mutex locks — persistent context was registered at
      ;;    scheduler start by init-carrier-fiber-context.
      ;;    Inline the fiber field writes (always needed) and carrier BSP
      ;;    (needed for binding stack scavenging).  The expensive C call
      ;;    (which computes __builtin_frame_address for carrier approx_sp)
      ;;    is only needed on migration — same-carrier resume can use the
      ;;    stale carrier_gc_info.control_stack_pointer safely because
      ;;    gencgc.c already scans the full carrier stack inline.
      (let ((ctx (sb-sys:int-sap
                   (sb-sys:sap-ref-word thread-sap
                     (ash sb-vm::thread-fiber-context-slot sb-vm:word-shift)))))
        (setf (sb-sys:sap-ref-word ctx (* 1 sb-vm:n-word-bytes))
              (fiber-control-stack-start fiber))
        (setf (sb-sys:sap-ref-word ctx (* 2 sb-vm:n-word-bytes))
              (fiber-control-stack-end fiber))
        (setf (sb-sys:sap-ref-word ctx (* 3 sb-vm:n-word-bytes))
              (fiber-binding-stack-start fiber))
        (setf (sb-sys:sap-ref-word ctx (* 8 sb-vm:n-word-bytes))
              carrier-bsp)
        (when migrated
          (update-fiber-gc-context
            (fiber-control-stack-start fiber)
            (fiber-control-stack-end fiber)
            (fiber-binding-stack-start fiber)
            carrier-bsp)))
      ;; 3. Set thread BSP to fiber's binding-stack-pointer
      (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
            (fiber-binding-stack-pointer fiber))
      ;; 3b. Restore fiber's catch/UWP blocks
      ;;     (0 for new fibers, saved values for suspended fibers)
      (setf (sb-sys:sap-ref-word thread-sap catch-offset)
            (fiber-saved-catch-block fiber))
      (setf (sb-sys:sap-ref-word thread-sap uwp-offset)
            (fiber-saved-unwind-protect-block fiber))
      ;; 3c. Update binding stack carrier values and replay TLS overlay.
      ;;     Skip entirely when fiber has no special variable bindings
      ;;     (BSP = binding-stack-start), avoiding hash table + array overhead.
      (when (/= (fiber-binding-stack-pointer fiber)
                (fiber-binding-stack-start fiber))
        ;; Update binding stack carrier values only on migration.
        ;; Same-carrier resume (common case) doesn't need this — the
        ;; outermost old_values already match the current carrier's TLS.
        (when migrated
          (update-binding-stack-carrier-values fiber thread-sap scheduler))
        ;; Replay fiber's TLS overlay
        (restore-fiber-tls-overlay fiber))
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
                  (fiber-control-stack-start fiber))
            ;; Also ensure binding_stack_pointer is current.
            ;; After fiber_switch returns (step 8), the gc_info must
            ;; cover the fiber's full binding stack because the thread
            ;; BSP will be the carrier's (set by yield/trampoline).
            (setf (sb-sys:sap-ref-word gc-info (* 4 sb-vm:n-word-bytes))
                  (fiber-binding-stack-pointer fiber)))))
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
      ;; 11. Stale fiber GC context is harmless — GC's guard checks
      ;;     verify ESP/BSP is within fiber range before using bounds.
      ;;     After yield, ESP is in carrier stack so guards fail safely.
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
  (let ((completed-normally nil))
    (unwind-protect
         (handler-case
             (let ((fn (fiber-function fiber)))
               (when fn
                 (setf (fiber-result fiber) (funcall fn)))
               (setf completed-normally t))
           (error (c)
             (setf (fiber-result fiber) c
                   (fiber-errorp fiber) t
                   completed-normally t)))
      ;; Detect cross-boundary non-local exit.  If neither the normal
      ;; path nor the error handler completed, a RETURN-FROM or GO
      ;; targeting a block/tag outside this fiber unwound through the
      ;; handler-case via %unwind (which operates below the condition
      ;; system).  Record the error so fiber-join / fiber-result can
      ;; report it instead of silently returning NIL.
      (unless completed-normally
        (setf (fiber-result fiber)
              (make-condition 'simple-error
                              :format-control
                              "Non-local exit (RETURN-FROM or GO) ~
                               crossed fiber boundary in fiber ~S"
                              :format-arguments (list (fiber-name fiber)))
              (fiber-errorp fiber) t))
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
          ;; Update gc_info's binding-stack-pointer before restoring
          ;; carrier BSP.  Same rationale as fiber-yield step 1c: after
          ;; carrier BSP is set, the active_fiber_context won't cover
          ;; the fiber's binding stack, so the gc_info must be accurate.
          (let ((gc-info (fiber-gc-info fiber)))
            (unless (sb-sys:sap= gc-info (sb-sys:int-sap 0))
              (sb-sys:without-gcing
                (setf (sb-sys:sap-ref-word gc-info (* 4 sb-vm:n-word-bytes))
                      current-bsp))))
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
          (error "fiber-trampoline: fiber_switch returned (unreachable)"))))))


;;;; ===== Register the Lisp trampoline with C =====

(define-alien-routine "set_fiber_run_trampoline"
    void (fun unsigned-long))

(defun install-fiber-trampoline ()
  "Register the Lisp fiber trampoline function with the C runtime."
  (set-fiber-run-trampoline
   (sb-kernel:get-lisp-obj-address #'fiber-trampoline)))


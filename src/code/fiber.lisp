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


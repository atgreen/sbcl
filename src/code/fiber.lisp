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

(define-alien-routine "fiber_switch"
    void
  (old-rsp-slot system-area-pointer)
  (new-rsp-slot system-area-pointer))

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

(defconstant +default-fiber-stack-size+ (* 64 1024))  ; 64KB
(defconstant +default-fiber-binding-stack-size+ (* 16 1024)) ; 16KB

;;;; ===== Fiber wait-info (tracks why a fiber is waiting, for idle hook) =====

(defstruct (fiber-wait-info
            (:constructor make-fiber-wait-info (fd direction)))
  (fd -1 :type fixnum)
  (direction :input :type (member :input :output)))

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
  ;; I/O wait info (fd + direction for idle hook batching)
  (wait-info nil :type (or null fiber-wait-info))
  ;; Pinning (prevents unmounting)
  (pin-count 0 :type fixnum)
  ;; GC info pointer (SAP to struct fiber_gc_info)
  (gc-info (int-sap 0) :type system-area-pointer)
  ;; C stack descriptor pointers (for freeing)
  (c-control-stack (int-sap 0) :type system-area-pointer)
  (c-binding-stack (int-sap 0) :type system-area-pointer))

;;;; ===== Fiber Scheduler struct =====

(defstruct (fiber-scheduler (:constructor %make-fiber-scheduler))
  (carrier nil)
  (run-queue nil :type list)
  (waiting nil :type list)
  (current-fiber nil :type (or null fiber))
  ;; Saved RSP for the scheduler's own stack frame (carrier thread stack)
  (saved-rsp 0 :type sb-vm:word)
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
  (registered-fds nil :type (or null hash-table)))

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
;;; These return SAPs pointing to the raw word slot in the struct.

(defun fiber-saved-rsp-sap (fiber)
  "Return a SAP pointing to the saved-rsp slot of FIBER."
  ;; The saved-rsp slot is an sb-vm:word slot in the defstruct.
  ;; We need the address of that slot in memory.
  ;; Use the primitive instance slot accessor.
  (sb-sys:int-sap
   (+ (sb-kernel:get-lisp-obj-address fiber)
      (- sb-vm:instance-pointer-lowtag)
      (* (+ 1 ; header word
            sb-vm:instance-data-start
            ;; slot index of saved-rsp in the fiber struct
            ;; We count: name=0, state=1, control-stack-start=2,
            ;;           control-stack-end=3, control-stack-size=4,
            ;;           saved-rsp=5
            5)
         sb-vm:n-word-bytes))))

(defun scheduler-saved-rsp-sap (scheduler)
  "Return a SAP pointing to the saved-rsp slot of SCHEDULER."
  (sb-sys:int-sap
   (+ (sb-kernel:get-lisp-obj-address scheduler)
      (- sb-vm:instance-pointer-lowtag)
      (* (+ 1 ; header word
            sb-vm:instance-data-start
            ;; slot index: carrier=0, run-queue=1, waiting=2,
            ;;             current-fiber=3, saved-rsp=4
            4)
         sb-vm:n-word-bytes))))

;;;; ===== Stack initialization =====

(define-alien-routine "get_fiber_entry_trampoline_addr"
    unsigned-long)

(defun fiber-entry-trampoline-address ()
  "Return the address of fiber_entry_trampoline (C/asm symbol)."
  (get-fiber-entry-trampoline-addr))

(defun initialize-fiber-stack (fiber)
  "Set up the initial stack frame for a brand-new fiber so that
fiber_switch 'returns' into fiber_entry_trampoline."
  (let* ((stack-start (fiber-control-stack-start fiber))
         (stack-size (fiber-control-stack-size fiber))
         (stack-top (+ stack-start stack-size))
         (trampoline-addr (fiber-entry-trampoline-address))
         (fiber-lispobj (sb-kernel:get-lisp-obj-address fiber)))
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
    ;;                     ^--- initial RSP (88 bytes from top)
    (let ((sap (sb-sys:int-sap stack-top)))
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
        ;; Initial RSP = stack_top - 88 (8 regs * 8 + return addr + padding)
        (setf (fiber-saved-rsp fiber) (- stack-top 88)))
      #-win32
      (progn
        (setf (sb-sys:sap-ref-word sap -48) 0)  ; -> r12
        (setf (sb-sys:sap-ref-word sap -56) 0)  ; -> r13
        (setf (sb-sys:sap-ref-word sap -64) 0)  ; -> r14
        (setf (sb-sys:sap-ref-word sap -72) 0)  ; -> r15
        ;; Initial RSP = stack_top - 72 (6 regs * 8 + return addr + padding)
        (setf (fiber-saved-rsp fiber) (- stack-top 72))))))

;;;; ===== Fiber creation =====

(defun make-fiber (function &key (name nil)
                                  (stack-size +default-fiber-stack-size+)
                                  (binding-stack-size +default-fiber-binding-stack-size+))
  "Create a new fiber that will execute FUNCTION when started.
FUNCTION should be a function of no arguments."
  (let ((cstack-sap (alloc-fiber-stack stack-size))
        (bstack-sap (alloc-fiber-binding-stack binding-stack-size)))
    (when (or (sb-sys:sap= cstack-sap (sb-sys:int-sap 0))
              (sb-sys:sap= bstack-sap (sb-sys:int-sap 0)))
      (error "Failed to allocate fiber stacks"))
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
      ;; Initialize the stack frame for fiber_switch
      (initialize-fiber-stack fiber)
      ;; Register for GC scanning immediately.  The initial stack frame
      ;; contains the fiber lispobj (a raw tagged pointer).  If GC runs
      ;; before this fiber starts, the conservative scanner must see that
      ;; reference to pin the fiber object in place.
      (register-fiber-for-gc-from-fiber fiber)
      fiber)))

(defun destroy-fiber (fiber)
  "Free the resources associated with FIBER. Only call on dead fibers."
  (unless (eq (fiber-state fiber) :dead)
    (error "Cannot destroy a fiber that is not dead"))
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

(defun save-fiber-tls-and-restore-carrier (fiber)
  "Save the fiber's current TLS values and restore the carrier's TLS values.
Walk the binding stack from bs_start to BSP (oldest to newest).  For each
unique TLS index, the FIRST (outermost) entry's old_value is the carrier's
original TLS value.  Save the fiber's current TLS value, then write the
carrier's value back to TLS.  Does NOT modify binding stack entries."
  (let* ((bsp (sb-sys:int-sap (fiber-binding-stack-pointer fiber)))
         (bs-start (sb-sys:int-sap (fiber-binding-stack-start fiber)))
         (thread-sap (current-thread-sap))
         ;; Collect unique {tls-index, carrier-old-value} pairs
         ;; Walking from oldest (bs_start) to newest (bsp) ensures
         ;; we capture the outermost (carrier's) old_value first.
         (entries nil)
         (seen nil))
    ;; Walk binding stack: entries are {value, tls_index} pairs, 2 words each.
    ;; Binding stack entry layout: [old_value @ offset 0] [tls_index @ offset 8]
    (let ((ptr bs-start))
      (loop while (sb-sys:sap< ptr bsp)
            do (let ((old-value (sb-sys:sap-ref-word ptr 0))
                     (tls-index (sb-sys:sap-ref-word ptr sb-vm:n-word-bytes)))
                 (when (and (plusp tls-index)
                            (not (member tls-index seen)))
                   (push tls-index seen)
                   (push (cons tls-index old-value) entries)))
               (setf ptr (sb-sys:sap+ ptr (* 2 sb-vm:n-word-bytes)))))
    ;; Build overlay arrays and swap TLS values
    (let ((n (length entries)))
      (if (zerop n)
          (setf (fiber-tls-indices fiber) nil
                (fiber-tls-values fiber) nil)
          (let ((indices (make-array n :element-type 'sb-vm:word))
                (values (make-array n :element-type 'sb-vm:word)))
            (loop for (tls-index . carrier-old-value) in (nreverse entries)
                  for i from 0
                  do ;; Save fiber's current TLS value
                     (setf (aref indices i) tls-index)
                     (setf (aref values i)
                           (sb-sys:sap-ref-word thread-sap tls-index))
                     ;; Restore carrier's TLS value
                     (setf (sb-sys:sap-ref-word thread-sap tls-index)
                           carrier-old-value))
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
                              sb-vm:word-shift)))
        ;; 1. Save current BSP as fiber's binding-stack-pointer
        (setf (fiber-binding-stack-pointer fiber)
              (sb-sys:sap-ref-word thread-sap bsp-offset))
        ;; 2. Save fiber's TLS values and restore carrier's TLS values.
        ;;    This does NOT call unbind_to_here (which would zero entries).
        ;;    Instead it directly swaps TLS slot values using the binding
        ;;    stack entries' old_values as the carrier's original state.
        (save-fiber-tls-and-restore-carrier fiber)
        ;; 3. Restore carrier's BSP
        (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
              (fiber-scheduler-carrier-bsp sched))
        ;; 4. Set fiber state and wake condition
        (setf (fiber-state fiber) :suspended
              (fiber-wake-condition fiber) wake-condition)
        ;; 5. Context switch to scheduler.
        ;;    NOTE: GC registration happens in resume-fiber-internal step 8,
        ;;    AFTER fiber_switch returns, when saved-rsp has the correct value.
        ;;    We must NOT register here because fiber_switch hasn't saved RSP yet.
        (fiber-switch (fiber-saved-rsp-sap fiber)
                      (scheduler-saved-rsp-sap sched))
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
      ;; 1. Save carrier's BSP
      (setf (fiber-scheduler-carrier-bsp scheduler) carrier-bsp)
      ;; 2. Set thread BSP to fiber's binding-stack-pointer
      (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
            (fiber-binding-stack-pointer fiber))
      ;; 3. Replay fiber's TLS overlay
      (restore-fiber-tls-overlay fiber)
      ;; 4. Unregister fiber from GC (it's now running on carrier)
      (unregister-fiber-gc-roots fiber)
      ;; 5. Register GC context: tell GC about the fiber's stack boundaries
      ;;    and register the carrier's suspended stack for scanning.
      (enter-fiber-gc-context
        (fiber-control-stack-start fiber)
        (fiber-control-stack-end fiber)
        (fiber-binding-stack-start fiber)
        carrier-stack-start carrier-stack-end
        carrier-bs-start carrier-bsp)
      ;; 6. Set current fiber
      (let ((old-fiber *current-fiber*)
            (old-sched *current-scheduler*))
        (setf *current-fiber* fiber
              *current-scheduler* scheduler)
        ;; 7. Context switch to fiber
        (fiber-switch (scheduler-saved-rsp-sap scheduler)
                      (fiber-saved-rsp-sap fiber))
        ;; Returns here when fiber yields or dies
        (setf *current-fiber* old-fiber
              *current-scheduler* old-sched))
      ;; 8. Leave fiber GC context (unregister carrier's stack)
      (leave-fiber-gc-context)
      ;; 9. Restore carrier's BSP (fiber's yield already did this,
      ;;    but be safe)
      (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
            (fiber-scheduler-carrier-bsp scheduler))
      ;; 10. Re-register fiber for GC if it suspended (not dead)
      (unless (eq (fiber-state fiber) :dead)
        (register-fiber-for-gc-from-fiber fiber)))))

;;;; ===== Fiber trampoline (called from C) =====

(defun fiber-trampoline (fiber)
  "Entry point for a new fiber. Called from fiber_run_and_finish via
the registered Lisp trampoline. Runs the fiber's function with
error handling, marks it dead, cleans up bindings, and switches
back to the scheduler."
  (let ((scheduler (fiber-scheduler fiber)))
    (unwind-protect
         (handler-case
             (let ((fn (fiber-function fiber)))
               (when fn
                 (setf (fiber-result fiber) (funcall fn))))
           (error (c)
             (setf (fiber-result fiber) c)))
      ;; Cleanup: mark dead, restore carrier TLS and BSP, switch back.
      ;; By this point, normal Lisp unbinding (let exits, handler-case
      ;; cleanup) has already undone the fiber's bindings and restored
      ;; TLS values.  BSP should be at bs_start.  But as a safety
      ;; measure, handle any remaining entries.
      (setf (fiber-state fiber) :dead)
      (let* ((thread-sap (current-thread-sap))
             (bsp-offset (ash sb-vm::thread-binding-stack-pointer-slot
                              sb-vm:word-shift))
             (current-bsp (sb-sys:sap-ref-word thread-sap bsp-offset))
             (bs-start (fiber-binding-stack-start fiber)))
        ;; If there are remaining binding stack entries, restore carrier
        ;; TLS values from outermost old_values (same approach as yield)
        (when (> current-bsp bs-start)
          (let ((bsp-sap (sb-sys:int-sap current-bsp))
                (start-sap (sb-sys:int-sap bs-start))
                (seen nil))
            (let ((ptr start-sap))
              (loop while (sb-sys:sap< ptr bsp-sap)
                    do (let ((old-value (sb-sys:sap-ref-word ptr 0))
                             (tls-index (sb-sys:sap-ref-word ptr sb-vm:n-word-bytes)))
                         (when (and (plusp tls-index)
                                    (not (member tls-index seen)))
                           (push tls-index seen)
                           (setf (sb-sys:sap-ref-word thread-sap tls-index)
                                 old-value)))
                    (setf ptr (sb-sys:sap+ ptr (* 2 sb-vm:n-word-bytes)))))))
        ;; Restore carrier's BSP
        (setf (sb-sys:sap-ref-word thread-sap bsp-offset)
              (fiber-scheduler-carrier-bsp scheduler)))
      ;; Switch back to scheduler
      (fiber-switch (fiber-saved-rsp-sap fiber)
                    (scheduler-saved-rsp-sap scheduler))
      ;; Should not reach here
      (error "fiber-trampoline: fiber_switch returned (unreachable)"))))

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
                :idle-hook idle-hook)))
    ;; Try to create platform-optimal event multiplexer
    #+(or linux bsd)
    (let ((efd #+linux (sb-unix:epoll-create1 0)
               #+bsd   (sb-unix:kqueue)))
      (when efd  ; nil on error
        (setf (fiber-scheduler-event-fd sched) efd
              (fiber-scheduler-registered-fds sched) (make-hash-table))))
    sched))

(defun submit-fiber (scheduler fiber)
  "Submit a fiber to a scheduler for execution."
  (setf (fiber-scheduler fiber) scheduler
        (fiber-state fiber) :runnable)
  (push fiber (fiber-scheduler-run-queue scheduler)))

(defun run-fiber-scheduler (scheduler)
  "Run the scheduler loop on the current thread. Returns when all
fibers have completed."
  (setf (fiber-scheduler-carrier scheduler) *current-thread*)
  ;; Install the C trampoline if not done yet
  (install-fiber-trampoline)
  (let ((*current-scheduler* scheduler)
        (*current-fiber* nil))
    (unwind-protect
         (loop
           ;; Check waiting fibers for wake conditions
           (let ((still-waiting nil))
             (dolist (f (fiber-scheduler-waiting scheduler))
               (if (and (fiber-wake-condition f)
                        (funcall (fiber-wake-condition f)))
                   (progn
                     (setf (fiber-state f) :runnable
                           (fiber-wake-condition f) nil)
                     (setf (fiber-scheduler-run-queue scheduler)
                           (nconc (fiber-scheduler-run-queue scheduler) (list f))))
                   (push f still-waiting)))
             (setf (fiber-scheduler-waiting scheduler) (nreverse still-waiting)))
           ;; Pick next fiber
           (let ((fiber (pop (fiber-scheduler-run-queue scheduler))))
             (cond
               (fiber
                (setf (fiber-scheduler-current-fiber scheduler) fiber)
                (ecase (fiber-state fiber)
                  (:created  (start-fiber scheduler fiber))
                  (:runnable (resume-fiber scheduler fiber)))
                ;; Fiber yielded or finished
                (setf (fiber-scheduler-current-fiber scheduler) nil)
                ;; Handle post-switch state
                (case (fiber-state fiber)
                  (:suspended
                   ;; Move to waiting list if has wake condition, else back to run queue
                   (if (fiber-wake-condition fiber)
                       (push fiber (fiber-scheduler-waiting scheduler))
                       (progn
                         (setf (fiber-state fiber) :runnable)
                         (setf (fiber-scheduler-run-queue scheduler)
                               (nconc (fiber-scheduler-run-queue scheduler) (list fiber))))))
                  (:dead
                   ;; Clean up dead fiber resources
                   (destroy-fiber fiber))))
               ;; No runnable fibers but some waiting
               ((fiber-scheduler-waiting scheduler)
                (let ((hook (fiber-scheduler-idle-hook scheduler)))
                  (if hook
                      (funcall hook scheduler)
                      (fiber-io-idle-hook scheduler))))
               ;; All fibers done
               (t (return)))))
      ;; Cleanup: close event multiplexer fd
      #+(or linux bsd)
      (let ((efd (fiber-scheduler-event-fd scheduler)))
        (when (plusp efd)
          (sb-unix:unix-close efd)
          (setf (fiber-scheduler-event-fd scheduler) -1))))))

;;;; ===== Convenience API =====

(defun fiber-sleep (seconds)
  "Sleep the current fiber for SECONDS (can be fractional).
Only works within a fiber context."
  (let ((deadline (+ (get-internal-real-time)
                     (truncate (* seconds internal-time-units-per-second)))))
    (fiber-yield (lambda () (>= (get-internal-real-time) deadline)))))

(defun fiber-alive-p (fiber)
  "Return true if the fiber has not yet finished."
  (not (eq (fiber-state fiber) :dead)))

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
Returns T if predicate satisfied, NIL on timeout."
  (let ((deadline (when timeout
                    (+ (get-internal-real-time)
                       (truncate (* timeout internal-time-units-per-second))))))
    (fiber-yield (if deadline
                     (lambda () (or (funcall predicate)
                                    (>= (get-internal-real-time) deadline)))
                     predicate))
    ;; After wake: check which condition triggered
    (if (and deadline (>= (get-internal-real-time) deadline)
             (not (funcall predicate)))
        nil   ; timeout
        t)))  ; predicate satisfied

;;;; ===== fiber-join =====

(defun fiber-join (target &key timeout)
  "Block current fiber until TARGET fiber completes. Returns fiber-result
or NIL on timeout."
  (when (eq target (current-fiber))
    (error "Fiber cannot join itself"))
  (unless (eq (fiber-state target) :dead)
    (fiber-park (lambda () (eq (fiber-state target) :dead)) :timeout timeout))
  (if (eq (fiber-state target) :dead)
      (fiber-result target)
      nil))

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
                        (sb-impl::relative-decoded-times stop-sec stop-usec)
                      (when (and (zerop sec) (not (plusp usec)))
                        (return-from %fiber-condition-wait nil))
                      (+ sec (/ usec 1000000.0d0))))))
            (unless (or (%try-mutex mutex)
                        (%fiber-grab-mutex-internal mutex remaining-secs))
              (return-from %fiber-condition-wait nil))
            ;; Success: return T, or (values T remaining-sec remaining-usec)
            (if stop-sec
                (multiple-value-bind (sec usec)
                    (sb-impl::relative-decoded-times stop-sec stop-usec)
                  (values t sec usec))
                t))))))

(defun %fiber-grab-mutex-internal (mutex timeout)
  "Internal fiber-aware mutex acquisition (no pin check).
Returns T on success, NIL on timeout."
  (loop
    (let ((ok (fiber-park
               (lambda ()
                 #+sb-futex (eql (mutex-state mutex) 0)
                 #-sb-futex (eql (mutex-%owner mutex) 0))
               :timeout timeout)))
      (unless ok (return nil))           ; timeout
      (when (%try-mutex mutex) (return t)))))

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
  ;; Fast check: already ready?
  (when (fd-ready-p fd direction)
    (return-from %fiber-wait-until-fd-usable t))
  ;; Park with fd-readiness wake condition, recording wait-info for idle hook
  (let ((fiber *current-fiber*)
        (scheduler *current-scheduler*))
    (setf (fiber-wait-info fiber) (make-fiber-wait-info fd direction))
    (%event-register scheduler fd direction)
    (unwind-protect
         (let ((result (fiber-park (lambda () (fd-ready-p fd direction))
                                   :timeout timeout)))
           (if result t nil))
      (%event-deregister scheduler fd direction)
      (setf (fiber-wait-info fiber) nil))))

;;;; ===== Event multiplexer registration (epoll/kqueue) =====

#+(or linux bsd)
(defun %event-register (scheduler fd direction)
  "Register fd/direction with the scheduler's event multiplexer."
  (let ((efd (fiber-scheduler-event-fd scheduler)))
    (when (minusp efd) (return-from %event-register))
    #+linux
    (let* ((table (fiber-scheduler-registered-fds scheduler))
           (new-events (if (eq direction :input)
                           sb-unix:epollin sb-unix:epollout))
           (current (gethash fd table 0))
           (combined (logior current new-events)))
      (if (zerop current)
          (sb-unix:epoll-ctl-add efd fd combined)
          (sb-unix:epoll-ctl-mod efd fd combined))
      (setf (gethash fd table) combined))
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
      (if (zerop remaining)
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
  "Scan waiting fibers and return the nearest deadline in milliseconds,
or NIL if no time-based deadlines exist."
  (let ((nearest nil))
    (dolist (f (fiber-scheduler-waiting scheduler))
      (declare (ignore f)))
    nearest))

;;; Shared fallback: register temporary handlers and call serve-event.
#-win32
(defun %batched-fd-poll/serve-event (scheduler timeout-ms)
  "Fallback: register temporary handlers and call serve-event."
  (let ((handlers nil)
        (timeout-sec (/ timeout-ms 1000.0d0)))
    (unwind-protect
         (progn
           (dolist (f (fiber-scheduler-waiting scheduler))
             (let ((info (fiber-wait-info f)))
               (when info
                 (push (add-fd-handler
                        (fiber-wait-info-fd info)
                        (fiber-wait-info-direction info)
                        (lambda (fd) (declare (ignore fd))))
                       handlers))))
           (when handlers
             (serve-event timeout-sec)))
      (dolist (h handlers)
        (remove-fd-handler h)))))

;;; Platform-specific %batched-fd-poll implementations
#+linux
(defun %batched-fd-poll (scheduler timeout-ms)
  "Wait for I/O events using epoll, falling back to serve-event."
  (let ((efd (fiber-scheduler-event-fd scheduler)))
    (if (minusp efd)
        (%batched-fd-poll/serve-event scheduler timeout-ms)
        (let ((buf (make-array 64 :element-type '(signed-byte 32))))
          (sb-sys:with-pinned-objects (buf)
            (let ((sap (sb-sys:vector-sap buf)))
              (sb-unix:epoll-wait efd sap 64
                                  (min timeout-ms 1000))))))))

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
    (dolist (f (fiber-scheduler-waiting scheduler))
      (let ((info (fiber-wait-info f)))
        (when (and info (fd-ready-p (fiber-wait-info-fd info)
                                     (fiber-wait-info-direction info)))
          (setf any-ready t))))
    (unless any-ready
      (sb-unix:nanosleep 0 (* (min timeout-ms 10) 1000000)))))

(defun fiber-io-idle-hook (scheduler)
  "Idle hook that polls fds from waiting fibers using a single poll/select call.
Called when no fibers are runnable but some are waiting."
  (let ((timeout-ms (or (compute-nearest-deadline scheduler) 100))
        (has-io-waiters nil))
    ;; Check if any waiting fibers have I/O wait info
    (dolist (f (fiber-scheduler-waiting scheduler))
      (when (fiber-wait-info f)
        (setf has-io-waiters t)
        (return)))
    (if has-io-waiters
        (%batched-fd-poll scheduler timeout-ms)
        ;; No I/O waiters; sleep briefly for timer-based waits
        (sb-unix:nanosleep 0 (* (min timeout-ms 10) 1000000)))))

;;;; ===== Exports =====

(export '(make-fiber
          destroy-fiber
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
          make-fiber-scheduler
          submit-fiber
          run-fiber-scheduler
          fiber-scheduler
          *current-fiber*
          *current-scheduler*
          fd-ready-p
          fiber-io-idle-hook
          fiber-wait-info
          fiber-wait-info-fd
          fiber-wait-info-direction))

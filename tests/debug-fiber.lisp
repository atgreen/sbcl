(in-package "CL-USER")

;; Debug: verify stack sizes and trigger GC
(format t "~%=== Debug: fiber stack sizes ===~%")
(format t "Default stack size: ~D bytes (~D KB)~%"
        sb-thread::+default-fiber-stack-size+
        (/ sb-thread::+default-fiber-stack-size+ 1024))

(let* ((sched (sb-thread::make-fiber-scheduler))
       (result nil))
  ;; First fiber: creates object and yields
  (sb-thread:submit-fiber sched
    (sb-thread:make-fiber
     (lambda ()
       (let ((fiber sb-thread:*current-fiber*))
         (format t "gc-fiber control-stack-start: ~X~%" (sb-thread::fiber-control-stack-start fiber))
         (format t "gc-fiber control-stack-end:   ~X~%" (sb-thread::fiber-control-stack-end fiber))
         (format t "gc-fiber control-stack-size:  ~D (~D KB)~%"
                 (sb-thread::fiber-control-stack-size fiber)
                 (/ (sb-thread::fiber-control-stack-size fiber) 1024))
         (let ((obj (list 1 2 3)))
           (format t "Created obj: ~A~%" obj)
           (sb-thread:fiber-yield)
           (format t "After yield, obj still: ~A~%" obj)
           (setf result obj))))
     :name "gc-fiber"))
  ;; Second fiber: prints its info and triggers GC
  (sb-thread:submit-fiber sched
    (sb-thread:make-fiber
     (lambda ()
       (let ((fiber sb-thread:*current-fiber*))
         (format t "~%gc-trigger control-stack-start: ~X~%" (sb-thread::fiber-control-stack-start fiber))
         (format t "gc-trigger control-stack-end:   ~X~%" (sb-thread::fiber-control-stack-end fiber))
         (format t "gc-trigger control-stack-size:  ~D (~D KB)~%"
                 (sb-thread::fiber-control-stack-size fiber)
                 (/ (sb-thread::fiber-control-stack-size fiber) 1024))
         ;; Print carrier thread stack info
         (let* ((thread-sap (sb-thread::current-thread-sap))
                (cs-start (sb-sys:sap-ref-word thread-sap
                            (ash sb-vm::thread-control-stack-start-slot sb-vm:word-shift)))
                (cs-end (sb-sys:sap-ref-word thread-sap
                          (ash sb-vm::thread-control-stack-end-slot sb-vm:word-shift))))
           (format t "~%Carrier control-stack-start: ~X~%" cs-start)
           (format t "Carrier control-stack-end:   ~X~%" cs-end)
           (format t "Carrier stack size: ~D (~D KB)~%"
                   (- cs-end cs-start) (/ (- cs-end cs-start) 1024)))
         (format t "~%About to trigger GC...~%")
         (force-output)
         (gc :full t)
         (format t "GC completed successfully!~%")))
     :name "gc-trigger"))
  (sb-thread::run-fiber-scheduler sched)
  (format t "Result: ~A~%" result)
  (assert (equal result '(1 2 3))))

(format t "~%=== Test PASSED ===~%")
(sb-ext:exit :code 0)

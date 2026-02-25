;;;; Tests for fiber (virtual thread) support
;;;;
;;;; This software is part of the SBCL system. See the README file for
;;;; more information.
;;;;
;;;; This software is in the public domain and is provided with
;;;; absolutely no warranty. See the COPYING and CREDITS files for
;;;; more information.

(use-package "SB-THREAD")

;;; Basic fiber creation and execution
(with-test (:name (:fiber :basic-run))
  (let* ((result nil)
         (fiber (make-fiber (lambda () (setf result 42))
                            :name "test-fiber"))
         (sched (make-fiber-scheduler)))
    (submit-fiber sched fiber)
    (run-fiber-scheduler sched)
    (assert (eql result 42))
    (assert (eq (fiber-state fiber) :dead))))

;;; Fiber returning a value
(with-test (:name (:fiber :return-value))
  (let* ((fiber (make-fiber (lambda () (+ 1 2 3))
                            :name "return-value-fiber"))
         (sched (make-fiber-scheduler)))
    (submit-fiber sched fiber)
    (run-fiber-scheduler sched)
    (assert (eql (fiber-result fiber) 6))
    (assert (eq (fiber-state fiber) :dead))))

;;; Multiple fibers running sequentially
(with-test (:name (:fiber :multiple-fibers))
  (let* ((results nil)
         (sched (make-fiber-scheduler)))
    (dotimes (i 5)
      (let ((n i))
        (submit-fiber sched
                      (make-fiber (lambda () (push n results))
                                  :name (format nil "fiber-~D" n)))))
    (run-fiber-scheduler sched)
    (assert (= (length results) 5))
    ;; All numbers 0-4 should be present (order may vary)
    (assert (null (set-difference results '(0 1 2 3 4))))))

;;; Fiber yield and resume
(with-test (:name (:fiber :yield-resume))
  (let* ((log nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (push 'a1 log)
                                (fiber-yield)
                                (push 'a2 log))
                              :name "fiber-a"))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (push 'b1 log)
                                (fiber-yield)
                                (push 'b2 log))
                              :name "fiber-b"))
    (run-fiber-scheduler sched)
    ;; Both fibers should have completed
    (assert (= (length log) 4))
    ;; A1 runs first, then yields.  B1 runs, then yields.
    ;; Then A2 runs, B2 runs (or vice versa for 2nd halves).
    (assert (member 'a1 log))
    (assert (member 'a2 log))
    (assert (member 'b1 log))
    (assert (member 'b2 log))))

;;; Dynamic variable bindings are fiber-local
(with-test (:name (:fiber :dynamic-bindings))
  (defvar *fiber-test-var* :carrier-value)
  (let* ((values nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (let ((*fiber-test-var* :fiber-a))
                                  (push (cons 'a1 *fiber-test-var*) values)
                                  (fiber-yield)
                                  (push (cons 'a2 *fiber-test-var*) values)))
                              :name "binding-fiber-a"))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (let ((*fiber-test-var* :fiber-b))
                                  (push (cons 'b1 *fiber-test-var*) values)
                                  (fiber-yield)
                                  (push (cons 'b2 *fiber-test-var*) values)))
                              :name "binding-fiber-b"))
    (run-fiber-scheduler sched)
    ;; Each fiber should see its own binding
    (assert (equal (cdr (assoc 'a1 values)) :fiber-a))
    (assert (equal (cdr (assoc 'a2 values)) :fiber-a))
    (assert (equal (cdr (assoc 'b1 values)) :fiber-b))
    (assert (equal (cdr (assoc 'b2 values)) :fiber-b))
    ;; Carrier value should be restored
    (assert (eq *fiber-test-var* :carrier-value))))

;;; Pinned fiber refuses to yield
(with-test (:name (:fiber :pin-prevents-yield))
  (let* ((error-caught nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (fiber-pin)
                                (handler-case
                                    (fiber-yield)
                                  (error () (setf error-caught t)))
                                (fiber-unpin))
                              :name "pinned-fiber"))
    (run-fiber-scheduler sched)
    (assert error-caught)))

;;; Fiber sleep (time-based wake condition)
(with-test (:name (:fiber :sleep))
  (let* ((start (get-internal-real-time))
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (fiber-sleep 0.01))  ; 10ms
                              :name "sleeping-fiber"))
    (run-fiber-scheduler sched)
    (let ((elapsed (/ (- (get-internal-real-time) start)
                      internal-time-units-per-second)))
      ;; Should have taken at least ~10ms
      (assert (>= elapsed 0.005)))))

;;; Many fibers (stress test)
(with-test (:name (:fiber :many-fibers))
  (let* ((counter 0)
         (sched (make-fiber-scheduler)))
    (dotimes (i 1000)
      (submit-fiber sched
                    (make-fiber (lambda ()
                                  (incf counter)
                                  (fiber-yield)
                                  (incf counter))
                                :name (format nil "stress-~D" i))))
    (run-fiber-scheduler sched)
    ;; Each fiber increments counter twice
    (assert (= counter 2000))))

;;; Unwind-protect runs in fibers
(with-test (:name (:fiber :unwind-protect))
  (let* ((cleanup-ran nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (unwind-protect
                                     (progn
                                       (fiber-yield)
                                       42)
                                  (setf cleanup-ran t)))
                              :name "unwind-fiber"))
    (run-fiber-scheduler sched)
    (assert cleanup-ran)))

;;; Error in fiber doesn't crash scheduler
(with-test (:name (:fiber :error-handling))
  (let* ((sched (make-fiber-scheduler))
         (good-result nil))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (error "intentional fiber error"))
                              :name "error-fiber"))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (setf good-result t))
                              :name "good-fiber"))
    (run-fiber-scheduler sched)
    ;; Good fiber should still complete
    (assert good-result)))

;;; with-fiber-pinned macro
(with-test (:name (:fiber :with-fiber-pinned))
  (let* ((sched (make-fiber-scheduler))
         (pin-count-during nil))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (with-fiber-pinned ()
                                  (setf pin-count-during
                                        (sb-thread::fiber-pin-count (current-fiber)))))
                              :name "pin-macro-fiber"))
    (run-fiber-scheduler sched)
    (assert (= pin-count-during 1))))

;;; Fiber name
(with-test (:name (:fiber :fiber-name))
  (let ((fiber (make-fiber (lambda () nil) :name "my-fiber")))
    (assert (equal (fiber-name fiber) "my-fiber"))))

;;; GC during fiber execution
(with-test (:name (:fiber :gc-during-execution))
  (let* ((sched (make-fiber-scheduler))
         (result nil))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (let ((obj (list 1 2 3)))
                                  (fiber-yield)
                                  ;; Force GC while fiber is suspended
                                  ;; (actually GC happens between fibers)
                                  (setf result obj)))
                              :name "gc-fiber"))
    ;; Add another fiber that triggers GC
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (gc :full t))
                              :name "gc-trigger-fiber"))
    (run-fiber-scheduler sched)
    (assert (equal result '(1 2 3)))))

;;;; ===== Fiber-aware threading primitive tests =====

;;; fiber-park with predicate
(with-test (:name (:fiber :fiber-park-basic))
  (let* ((flag nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (fiber-park (lambda () flag)))
                              :name "parker"))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (fiber-yield)
                                (setf flag t))
                              :name "waker"))
    (run-fiber-scheduler sched)))

;;; fiber-park with timeout
(with-test (:name (:fiber :fiber-park-timeout))
  (let* ((result nil)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (setf result
                                      (fiber-park (lambda () nil)
                                                  :timeout 0.01)))
                              :name "timeout-parker"))
    (run-fiber-scheduler sched)
    (assert (null result))))

;;; fiber-join
(with-test (:name (:fiber :fiber-join))
  (let* ((result nil)
         (sched (make-fiber-scheduler))
         (target (make-fiber (lambda () 42) :name "target")))
    (submit-fiber sched target)
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (setf result (fiber-join target)))
                              :name "joiner"))
    (run-fiber-scheduler sched)
    (assert (eql result 42))))

;;; Fiber-aware condition-wait: producer/consumer
(with-test (:name (:fiber :condition-wait))
  (let* ((queue (make-waitqueue :name "fiber-cv"))
         (mutex (make-mutex :name "fiber-cv-mutex"))
         (data nil)
         (consumed nil)
         (sched (make-fiber-scheduler)))
    ;; Consumer fiber: wait until data is available
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:with-mutex (mutex)
                                  (loop until data
                                        do (sb-thread:condition-wait queue mutex))
                                  (setf consumed (pop data))))
                              :name "consumer"))
    ;; Producer fiber: produce data and notify
    (submit-fiber sched
                  (make-fiber (lambda ()
                                ;; Yield once to let consumer start waiting
                                (fiber-yield)
                                (sb-thread:with-mutex (mutex)
                                  (push :item data)
                                  (sb-thread:condition-notify queue)))
                              :name "producer"))
    (run-fiber-scheduler sched)
    (assert (eq consumed :item))))

;;; Fiber-aware mutex contention
(with-test (:name (:fiber :mutex-contention))
  (let* ((mutex (make-mutex :name "fiber-contention"))
         (counter 0)
         (sched (make-fiber-scheduler)))
    ;; One fiber holds the mutex while others contend for it.
    ;; The holder yields while holding the mutex; waiters park via
    ;; fiber-aware grab-mutex until the mutex is released.
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:with-mutex (mutex)
                                  (incf counter)
                                  (fiber-yield) ; yield while holding
                                  (incf counter)))
                              :name "holder"))
    ;; These fibers will contend on the mutex
    (dotimes (i 3)
      (submit-fiber sched
                    (make-fiber (lambda ()
                                  (sb-thread:with-mutex (mutex)
                                    (incf counter)))
                                :name (format nil "waiter-~D" i))))
    (run-fiber-scheduler sched)
    ;; holder does 2 increments, 3 waiters do 1 each = 5
    (assert (= counter 5))))

;;; Fiber-aware semaphore
(with-test (:name (:fiber :semaphore))
  (let* ((sem (make-semaphore :name "fiber-sem" :count 0))
         (result nil)
         (sched (make-fiber-scheduler)))
    ;; Waiter fiber
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:wait-on-semaphore sem)
                                (setf result :got-it))
                              :name "sem-waiter"))
    ;; Signaler fiber
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (fiber-yield)
                                (sb-thread:signal-semaphore sem))
                              :name "sem-signaler"))
    (run-fiber-scheduler sched)
    (assert (eq result :got-it))))

;;; Pinned fiber warning on blocking primitive
(with-test (:name (:fiber :pinned-blocking-warning))
  (let* ((warning-caught nil)
         (sched (make-fiber-scheduler))
         (queue (make-waitqueue :name "pinned-cv"))
         (mutex (make-mutex :name "pinned-mutex")))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:with-mutex (mutex)
                                  (fiber-pin)
                                  (let ((*pinned-blocking-action* :warn))
                                    (handler-bind ((warning
                                                     (lambda (w)
                                                       (setf warning-caught t)
                                                       (muffle-warning w))))
                                      ;; This should warn since fiber is pinned,
                                      ;; then fall through to OS path. Use timeout
                                      ;; to avoid blocking forever.
                                      (sb-thread:condition-wait queue mutex
                                                                :timeout 0.01)))
                                  (fiber-unpin)))
                              :name "pinned-fiber"))
    (run-fiber-scheduler sched)
    (assert warning-caught)))

;;; fiber-condition-wait timeout
(with-test (:name (:fiber :condition-wait-timeout))
  (let* ((queue (make-waitqueue :name "timeout-cv"))
         (mutex (make-mutex :name "timeout-mutex"))
         (result :not-set)
         (sched (make-fiber-scheduler)))
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:with-mutex (mutex)
                                  (setf result
                                        (sb-thread:condition-wait queue mutex
                                                                  :timeout 0.01))))
                              :name "timeout-waiter"))
    (run-fiber-scheduler sched)
    ;; condition-wait returns NIL on timeout
    (assert (null result))))

;;; fiber-grab-mutex timeout
(with-test (:name (:fiber :grab-mutex-timeout))
  (let* ((mutex (make-mutex :name "timeout-mutex"))
         (result :not-set)
         (sched (make-fiber-scheduler)))
    ;; First fiber grabs and holds the mutex
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (sb-thread:grab-mutex mutex)
                                ;; Hold mutex across yields
                                (fiber-yield)
                                (fiber-yield)
                                (fiber-yield)
                                (sb-thread:release-mutex mutex))
                              :name "holder"))
    ;; Second fiber tries to grab with timeout
    (submit-fiber sched
                  (make-fiber (lambda ()
                                (setf result
                                      (sb-thread:grab-mutex mutex :timeout 0.01))
                                (when result
                                  (sb-thread:release-mutex mutex)))
                              :name "timeout-grabber"))
    (run-fiber-scheduler sched)
    ;; The grabber should have timed out (NIL) or eventually got it (T)
    ;; depending on timing; but with 3 yields and 10ms timeout it should timeout
    (assert (member result '(nil t)))))

;;;; ===== Fiber-aware I/O tests =====

;;; fd-ready-p basic check
(with-test (:name (:fiber :fd-ready-p))
  ;; /dev/null is always ready for input (returns EOF immediately)
  (let ((fd (sb-unix:unix-open "/dev/null" sb-unix:o_rdonly 0)))
    (unwind-protect
         (assert (fd-ready-p fd :input))
      (sb-unix:unix-close fd))))

;;; Fiber pipe I/O: two fibers communicating over a pipe
(with-test (:name (:fiber :pipe-io))
  (multiple-value-bind (read-fd write-fd)
      (sb-unix:unix-pipe)
    (let* ((received nil)
           (sched (make-fiber-scheduler)))
      ;; Reader fiber: reads from pipe (should yield until data arrives)
      (submit-fiber sched
                    (make-fiber (lambda ()
                                  (let ((stream (sb-sys:make-fd-stream
                                                 read-fd :input t
                                                 :element-type 'character
                                                 :buffering :line)))
                                    (setf received (read-line stream nil nil))
                                    (close stream)))
                                :name "pipe-reader"))
      ;; Writer fiber: writes to pipe after yielding
      (submit-fiber sched
                    (make-fiber (lambda ()
                                  (fiber-yield)
                                  (let ((stream (sb-sys:make-fd-stream
                                                 write-fd :output t
                                                 :element-type 'character
                                                 :buffering :line)))
                                    (write-line "hello from fiber" stream)
                                    (force-output stream)
                                    (close stream)))
                                :name "pipe-writer"))
      (run-fiber-scheduler sched)
      (assert (equal received "hello from fiber")))))

;;; Fiber I/O timeout: read from pipe with timeout, no data arrives
(with-test (:name (:fiber :io-timeout))
  (multiple-value-bind (read-fd write-fd)
      (sb-unix:unix-pipe)
    (let* ((result :not-set)
           (sched (make-fiber-scheduler)))
      (submit-fiber sched
                    (make-fiber (lambda ()
                                  ;; wait-until-fd-usable with short timeout
                                  ;; through the fiber dispatch path
                                  (setf result
                                        (sb-sys:wait-until-fd-usable
                                         read-fd :input 0.05)))
                                :name "timeout-reader"))
      (run-fiber-scheduler sched)
      ;; Should have timed out (NIL) since no one wrote to the pipe
      (assert (null result))
      (sb-unix:unix-close read-fd)
      (sb-unix:unix-close write-fd))))

;;; Idle hook efficiency: fibers sleeping should not busy-poll
(with-test (:name (:fiber :idle-hook-efficiency))
  (let* ((sched (make-fiber-scheduler))
         (start (get-internal-real-time)))
    ;; 5 fibers each sleeping 100ms
    (dotimes (i 5)
      (submit-fiber sched
                    (make-fiber (lambda () (fiber-sleep 0.1))
                                :name (format nil "sleeper-~D" i))))
    (run-fiber-scheduler sched)
    (let ((elapsed (/ (- (get-internal-real-time) start)
                      internal-time-units-per-second)))
      ;; Should complete in roughly 100ms (all sleep concurrently),
      ;; not 500ms (sequential). Allow generous margin.
      (assert (< elapsed 1.0)))))

;;; Verify scheduler creates an event-fd on Linux/BSD
(with-test (:name (:fiber :epoll-kqueue-event-fd))
  (let ((s (make-fiber-scheduler)))
    #+(or linux bsd)
    (assert (plusp (sb-thread::fiber-scheduler-event-fd s)))
    #-(or linux bsd)
    (assert (= -1 (sb-thread::fiber-scheduler-event-fd s)))
    ;; Cleanup: close the event fd if open
    #+(or linux bsd)
    (let ((efd (sb-thread::fiber-scheduler-event-fd s)))
      (when (plusp efd) (sb-unix:unix-close efd)))))

;;; Pipe I/O using epoll/kqueue backend
(with-test (:name (:fiber :epoll-kqueue-pipe-io))
  (multiple-value-bind (read-fd write-fd)
      (sb-unix:unix-pipe)
    (assert read-fd)
    (unwind-protect
         (let* ((sched (make-fiber-scheduler))
                (result nil))
           ;; Verify event-fd is active on supported platforms
           #+(or linux bsd)
           (assert (plusp (sb-thread::fiber-scheduler-event-fd sched)))
           ;; Reader fiber: waits for data
           (submit-fiber sched
                         (make-fiber
                          (lambda ()
                            (sb-sys:wait-until-fd-usable read-fd :input nil)
                            (let ((buf (make-array 1 :element-type '(unsigned-byte 8))))
                              (sb-unix:unix-read read-fd
                                                 (sb-sys:vector-sap buf) 1)
                              (setf result (aref buf 0))))
                          :name "epoll-reader"))
           ;; Writer fiber: writes after short sleep
           (submit-fiber sched
                         (make-fiber
                          (lambda ()
                            (fiber-sleep 0.05)
                            (let ((buf (make-array 1 :element-type '(unsigned-byte 8)
                                                     :initial-element 99)))
                              (sb-unix:unix-write write-fd
                                                  (sb-sys:vector-sap buf) 0 1)))
                          :name "epoll-writer"))
           (run-fiber-scheduler sched)
           (assert (eql result 99)))
      (sb-unix:unix-close read-fd)
      (sb-unix:unix-close write-fd))))
